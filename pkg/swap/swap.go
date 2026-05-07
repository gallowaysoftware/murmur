// Package swap manages atomic state-table version pointers for backfill cutover.
//
// The lambda-replay backfill pattern:
//
//  1. Live pipeline writes to "<alias>_v<N>" (e.g. "page_views_v3").
//  2. Backfill replay job runs into a fresh "<alias>_v<N+1>" — independent of live.
//  3. When replay completes, the operator (or automation) calls Manager.SetActive to
//     atomically advance the alias pointer.
//  4. Query layer rolls / refreshes; clients now see results from v<N+1>.
//  5. Old v<N> is dropped after a grace period.
//
// The control table is a single DDB table holding one row per alias:
//
//	pk:    "<alias>"   (S)
//	ver:   <version>   (N)
//	at:    <unix-ms>   (N)  — last update timestamp
//
// SetActive uses a conditional UpdateExpression so concurrent swap attempts can't
// regress (only ver > current-ver is accepted).
package swap

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

const (
	attrAlias   = "pk"
	attrVersion = "ver"
	attrAt      = "at"
)

// TableName renders "<alias>_v<version>" for state-table naming consistency.
func TableName(alias string, version int64) string {
	return fmt.Sprintf("%s_v%d", alias, version)
}

// Manager reads and writes version pointers in the control table.
type Manager struct {
	client       *dynamodb.Client
	controlTable string
}

// New constructs a Manager. The control table must already exist; CreateControlTable
// is the convenience helper.
func New(client *dynamodb.Client, controlTable string) *Manager {
	return &Manager{client: client, controlTable: controlTable}
}

// ErrNoActiveVersion is returned by GetActive when no pointer has been set yet for
// the alias.
var ErrNoActiveVersion = errors.New("swap: no active version for alias")

// ErrVersionRegressed is returned by SetActive when the requested version is not
// strictly greater than the current active version.
var ErrVersionRegressed = errors.New("swap: cannot regress version")

// GetActive returns the current active version for alias. Returns ErrNoActiveVersion
// if none has been set.
func (m *Manager) GetActive(ctx context.Context, alias string) (int64, error) {
	out, err := m.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: &m.controlTable,
		Key: map[string]types.AttributeValue{
			attrAlias: &types.AttributeValueMemberS{Value: alias},
		},
		ConsistentRead: aws.Bool(true),
	})
	if err != nil {
		return 0, fmt.Errorf("swap GetItem %s: %w", m.controlTable, err)
	}
	if len(out.Item) == 0 {
		return 0, ErrNoActiveVersion
	}
	verAttr, ok := out.Item[attrVersion].(*types.AttributeValueMemberN)
	if !ok {
		return 0, fmt.Errorf("swap %s: %s missing or non-numeric", alias, attrVersion)
	}
	v, err := strconv.ParseInt(verAttr.Value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("swap %s: parse version: %w", alias, err)
	}
	return v, nil
}

// SetActive atomically advances the alias pointer to newVersion. Refuses to regress —
// returns ErrVersionRegressed when newVersion is not strictly greater than the current
// version. The first SetActive call (no prior version) always succeeds.
func (m *Manager) SetActive(ctx context.Context, alias string, newVersion int64) error {
	now := time.Now().UnixMilli()
	_, err := m.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: &m.controlTable,
		Key: map[string]types.AttributeValue{
			attrAlias: &types.AttributeValueMemberS{Value: alias},
		},
		UpdateExpression:    aws.String("SET #v = :nv, #t = :now"),
		ConditionExpression: aws.String("attribute_not_exists(#v) OR #v < :nv"),
		ExpressionAttributeNames: map[string]string{
			"#v": attrVersion,
			"#t": attrAt,
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":nv":  &types.AttributeValueMemberN{Value: strconv.FormatInt(newVersion, 10)},
			":now": &types.AttributeValueMemberN{Value: strconv.FormatInt(now, 10)},
		},
	})
	if err != nil {
		var ccf *types.ConditionalCheckFailedException
		if errors.As(err, &ccf) {
			return ErrVersionRegressed
		}
		return fmt.Errorf("swap UpdateItem %s: %w", m.controlTable, err)
	}
	return nil
}

// Resolve combines GetActive with TableName: returns "<alias>_v<active>" or, if no
// version has been set, "<alias>_v1" as the implicit-bootstrap convention.
func (m *Manager) Resolve(ctx context.Context, alias string) (string, error) {
	v, err := m.GetActive(ctx, alias)
	if errors.Is(err, ErrNoActiveVersion) {
		return TableName(alias, 1), nil
	}
	if err != nil {
		return "", err
	}
	return TableName(alias, v), nil
}

// CreateControlTable creates the control table with the schema expected by Manager.
// Production tables should be created via Terraform; this helper exists for tests
// and dev-loop convenience.
func CreateControlTable(ctx context.Context, client *dynamodb.Client, controlTable string) error {
	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: &controlTable,
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String(attrAlias), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String(attrAlias), KeyType: types.KeyTypeHash},
		},
		BillingMode: types.BillingModePayPerRequest,
	})
	var inUse *types.ResourceInUseException
	if errors.As(err, &inUse) {
		return nil
	}
	return err
}
