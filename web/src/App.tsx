import { Route, Routes } from 'react-router-dom'
import { Layout } from './components/Layout'
import { Pipelines } from './pages/Pipelines'
import { PipelineDetail } from './pages/PipelineDetail'
import { QueryConsole } from './pages/QueryConsole'
import { About } from './pages/About'

function App() {
  return (
    <Routes>
      <Route element={<Layout />}>
        <Route index element={<Pipelines />} />
        <Route path="pipelines/:name" element={<PipelineDetail />} />
        <Route path="query" element={<QueryConsole />} />
        <Route path="about" element={<About />} />
      </Route>
    </Routes>
  )
}

export default App
