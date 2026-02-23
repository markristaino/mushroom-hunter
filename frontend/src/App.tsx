import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import {
  MapContainer,
  Popup,
  Rectangle,
  TileLayer,
  useMapEvents,
} from 'react-leaflet'
import type { LatLngBounds } from 'leaflet'
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
  canopy_pct_nlcd?: number
}

type NowcastResponse = {
  species_id: string
  as_of: string
  count: number
  cells: NowcastCell[]
}

const API_BASE = import.meta.env.VITE_API_URL ?? 'http://127.0.0.1:8000'
const HALF_STEP = 0.0015

// ---------------------------------------------------------------------------
// MapBoundsWatcher — fires onBoundsChange whenever the viewport moves/zooms
// ---------------------------------------------------------------------------
function MapBoundsWatcher({
  onBoundsChange,
}: {
  onBoundsChange: (bounds: LatLngBounds) => void
}) {
  const map = useMapEvents({
    moveend() {
      onBoundsChange(map.getBounds())
    },
    zoomend() {
      onBoundsChange(map.getBounds())
    },
  })

  useEffect(() => {
    onBoundsChange(map.getBounds())
  }, []) // eslint-disable-line react-hooks/exhaustive-deps

  return null
}

// ---------------------------------------------------------------------------
// Main App
// ---------------------------------------------------------------------------
function App() {
  const [species, setSpecies] = useState<string[]>([])
  const [selectedSpecies, setSelectedSpecies] = useState<string>('chanterelle')
  const [cells, setCells] = useState<NowcastCell[]>([])
  const [asOf, setAsOf] = useState<string>('')
  const [loading, setLoading] = useState<boolean>(false)
  const [error, setError] = useState<string>('')
  const [bounds, setBounds] = useState<LatLngBounds | null>(null)
  const debounceRef = useRef<ReturnType<typeof setTimeout> | null>(null)

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

  const fetchNowcastRefined = useCallback(
    async (activeSpecies: string, viewport: LatLngBounds) => {
      setLoading(true)
      setError('')
      const params = new URLSearchParams({
        species_id: activeSpecies,
        min_score: '0.3',
        min_lat: viewport.getSouth().toFixed(6),
        max_lat: viewport.getNorth().toFixed(6),
        min_lon: viewport.getWest().toFixed(6),
        max_lon: viewport.getEast().toFixed(6),
      })
      try {
        const response = await fetch(`${API_BASE}/api/nowcast_refined?${params}`)
        if (response.status === 400) {
          const data = await response.json()
          setError(data.detail ?? 'Too many cells — zoom in further')
          setCells([])
          return
        }
        if (response.status === 503) {
          setError('300m data not ready — run: python3 -m app.pipelines.habitat_refinement')
          setCells([])
          return
        }
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

  // Debounced fetch on viewport or species change
  useEffect(() => {
    if (!bounds) return
    if (debounceRef.current) clearTimeout(debounceRef.current)
    debounceRef.current = setTimeout(() => {
      fetchNowcastRefined(selectedSpecies, bounds)
    }, 400)
    return () => {
      if (debounceRef.current) clearTimeout(debounceRef.current)
    }
  }, [selectedSpecies, bounds, fetchNowcastRefined])

  const cellBounds = useCallback(
    (lat: number, lng: number): [[number, number], [number, number]] => [
      [lat - HALF_STEP, lng - HALF_STEP],
      [lat + HALF_STEP, lng + HALF_STEP],
    ],
    [],
  )

  // Opacity scales with score: 0.3 → 0.25, 1.0 → 0.85
  const patchOpacity = useCallback(
    (score: number) => 0.25 + ((score - 0.3) / 0.7) * 0.6,
    [],
  )

  const summary = useMemo(() => {
    const total = cells.length
    const excellent = cells.filter((cell) => cell.score >= 0.95).length
    return { total, excellent }
  }, [cells])

  const handleBoundsChange = useCallback((newBounds: LatLngBounds) => {
    setBounds(newBounds)
  }, [])

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
        {loading && <p className="hint">Loading…</p>}
        {error && <p className="error">{error}</p>}
        <section className="legend">
          <p>Map key</p>
          <ul>
            <li>
              <span className="swatch" style={{ backgroundColor: '#4ade80', opacity: 0.85 }} />
              Favorable conditions (score &ge; 0.3)
            </li>
          </ul>
          <p className="hint">Brighter patches = closer to ideal. Pan or zoom to load cells for the current view.</p>
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
          <MapBoundsWatcher onBoundsChange={handleBoundsChange} />
          {cells.map((cell) => (
            <Rectangle
              key={cell.cell_id}
              bounds={cellBounds(cell.latitude, cell.longitude)}
              pathOptions={{
                color: '#16a34a',
                weight: 0.5,
                fillColor: '#4ade80',
                fillOpacity: patchOpacity(cell.score),
              }}
            >
              <Popup>
                <strong>{cell.cell_id}</strong>
                <br />Score: {cell.score.toFixed(2)}
                {cell.canopy_pct_nlcd != null && (
                  <><br />Canopy (NLCD): {cell.canopy_pct_nlcd}%</>
                )}
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
