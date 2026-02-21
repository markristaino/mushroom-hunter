import { useCallback, useEffect, useMemo, useState } from 'react'
import {
  MapContainer,
  Popup,
  Rectangle,
  TileLayer,
} from 'react-leaflet'
import 'leaflet/dist/leaflet.css'
import './App.css'

type SpeciesResponse = { species: string[] }

type ScoreComponent = {
  name: string
  passed: boolean
  detail: string
  weight: number
}

type NowcastCell = {
  cell_id: string
  latitude: number
  longitude: number
  score: number
  components: ScoreComponent[]
  last_observation: string
}

type NowcastResponse = {
  species_id: string
  as_of: string
  count: number
  cells: NowcastCell[]
}

const API_BASE = import.meta.env.VITE_API_URL ?? 'http://127.0.0.1:8000'

function App() {
  const [species, setSpecies] = useState<string[]>([])
  const [selectedSpecies, setSelectedSpecies] = useState<string>('chanterelle')
  const [cells, setCells] = useState<NowcastCell[]>([])
  const [asOf, setAsOf] = useState<string>('')
  const [loading, setLoading] = useState<boolean>(true)
  const [error, setError] = useState<string>('')

  const fetchSpecies = useCallback(async () => {
    try {
      const response = await fetch(`${API_BASE}/api/species`)
      if (!response.ok) throw new Error('Failed to load species list')
      const payload: SpeciesResponse = await response.json()
      setSpecies(payload.species)
      if (!payload.species.includes(selectedSpecies) && payload.species.length) {
        setSelectedSpecies(payload.species[0])
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Unknown error')
    }
  }, [selectedSpecies])

  useEffect(() => {
    fetchSpecies()
  }, [fetchSpecies])

  const fetchNowcast = useCallback(
    async (activeSpecies: string) => {
      setLoading(true)
      setError('')
      try {
        const response = await fetch(
          `${API_BASE}/api/nowcast?species_id=${encodeURIComponent(activeSpecies)}&min_score=0.8`,
        )
        if (!response.ok) throw new Error('Failed to load nowcast data')
        const payload: NowcastResponse = await response.json()
        setCells(payload.cells)
        setAsOf(payload.as_of)
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Unknown error')
      } finally {
        setLoading(false)
      }
    },
    [],
  )

  useEffect(() => {
    fetchNowcast(selectedSpecies)
  }, [fetchNowcast, selectedSpecies])

  // Grid cell half-size in degrees (matches pipeline GRID_STEP / 2)
  const HALF_STEP = 0.025

  const cellBounds = useCallback(
    (lat: number, lng: number): [[number, number], [number, number]] => [
      [lat - HALF_STEP, lng - HALF_STEP],
      [lat + HALF_STEP, lng + HALF_STEP],
    ],
    [],
  )

  // Opacity scales with score (0.8 → 0.4 opacity, 1.0 → 0.85 opacity)
  const patchOpacity = useCallback((score: number) => 0.4 + (score - 0.8) * 2.25, [])

  const summary = useMemo(() => {
    const total = cells.length
    const excellent = cells.filter((cell) => cell.score >= 0.95).length
    return { total, excellent }
  }, [cells])

  return (
    <div className="app">
      <aside className="panel">
        <header>
          <h1>Mushroom Nowcast</h1>
          <p>Visualize deterministic suitability scores for the Pacific Northwest.</p>
        </header>
        <label className="input-label">
          Species
          <select
            value={selectedSpecies}
            onChange={(event) => setSelectedSpecies(event.target.value)}
            disabled={!species.length}
          >
            {species.map((id) => (
              <option key={id} value={id}>
                {id.replace('-', ' ')}
              </option>
            ))}
          </select>
        </label>
        <div className="status-card">
          <p className="status-label">Last refreshed</p>
          <p className="status-value">{asOf ? new Date(asOf).toLocaleString() : '—'}</p>
          <div className="status-metrics">
            <div>
              <span className="metric-value">{summary.excellent}</span>
              <span className="metric-label">excellent spots</span>
            </div>
            <div>
              <span className="metric-value">{summary.total}</span>
              <span className="metric-label">favorable patches</span>
            </div>
          </div>
        </div>
        {loading && <p className="hint">Loading nowcast data…</p>}
        {error && <p className="error">{error}</p>}
        <section className="legend">
          <p>Map key</p>
          <ul>
            <li>
              <span className="swatch" style={{ backgroundColor: '#4ade80', opacity: 0.85 }} />
              Favorable conditions (score &ge; 0.8)
            </li>
          </ul>
          <p className="hint">Brighter patches = closer to ideal conditions. No patches = not in season or conditions unmet.</p>
        </section>
      </aside>
      <main className="map-wrapper">
        <MapContainer
          center={[46.5, -122]}
          zoom={6}
          className="map-container"
          scrollWheelZoom
        >
          <TileLayer url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png" attribution="&copy; OpenStreetMap" />
          {cells.map((cell) => (
            <Rectangle
              key={cell.cell_id}
              bounds={cellBounds(cell.latitude, cell.longitude)}
              pathOptions={{ color: '#16a34a', weight: 1.5, fillColor: '#4ade80', fillOpacity: patchOpacity(cell.score) }}
            >
              <Popup>
                <strong>{cell.cell_id}</strong>
                <br />Score: {cell.score.toFixed(2)}
                <br />
                {cell.components.map((component) => (
                  <span key={component.name} className="component-row">
                    {component.name}: {component.passed ? '✓' : '✗'}
                  </span>
                ))}
                <br />Last observation: {new Date(cell.last_observation).toLocaleString()}
              </Popup>
            </Rectangle>
          ))}
        </MapContainer>
      </main>
    </div>
  )
}

export default App
