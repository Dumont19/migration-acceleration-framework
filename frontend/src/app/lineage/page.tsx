'use client'

import { useEffect, useRef, useState, useCallback } from 'react'
import * as d3 from 'd3'
import { datastageApi } from '@/lib/api'
import type { LineageGraph, LineageNode, LineageEdge } from '@/lib/api'
import { SectionLabel, EmptyState, StatCard } from '@/components/ui'

// ── Constants ─────────────────────────────────────────────────────────────────

const NODE_COLORS: Record<string, string> = {
  source: '#4488ff',
  job:    '#00ff88',
  target: '#ffcc00',
}

const NODE_RADIUS: Record<string, number> = {
  source: 18,
  job:    24,
  target: 18,
}

type D3Node = LineageNode & d3.SimulationNodeDatum
type D3Link = LineageEdge & { source: string | D3Node; target: string | D3Node }

// ── Component ─────────────────────────────────────────────────────────────────

export default function LineagePage() {
  const svgRef      = useRef<SVGSVGElement>(null)
  const fileInputRef = useRef<HTMLInputElement>(null)

  const [graph,    setGraph]    = useState<LineageGraph | null>(null)
  const [fileName, setFileName] = useState('')
  const [xmlSize,  setXmlSize]  = useState(0)
  const [loading,  setLoading]  = useState(false)
  const [selected, setSelected] = useState<LineageNode | null>(null)
  const [error,    setError]    = useState<string | null>(null)

  // ── File handling ───────────────────────────────────────────────────────────

  const handleFileChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0]
    if (!file) return
    setFileName(file.name)
    setXmlSize(file.size)
    setGraph(null)
    setSelected(null)
    setError(null)
  }

  /**
   * Sempre envia como File via multipart — consistente com job_docs e backend FastAPI.
   */
  const buildForm = useCallback((): FormData | null => {
    const file = fileInputRef.current?.files?.[0]
    if (!file) return null
    const form = new FormData()
    form.append('file', file)
    return form
  }, [])

  // ── Generate lineage ────────────────────────────────────────────────────────

  const generateLineage = useCallback(async () => {
    const form = buildForm()
    if (!form) { setError('Load a .dsx / .xml file first'); return }
    setError(null)
    setLoading(true)
    setSelected(null)
    try {
      const data = await datastageApi.lineage(form)
      setGraph(data)
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : 'Failed to parse XML')
    } finally {
      setLoading(false)
    }
  }, [buildForm])

  // ── D3 force simulation ─────────────────────────────────────────────────────

  useEffect(() => {
    if (!graph || !svgRef.current) return

    const svg    = d3.select(svgRef.current)
    svg.selectAll('*').remove()

    const width  = svgRef.current.clientWidth || 800
    const height = 480

    const container = svg.append('g')
    svg.call(
      d3.zoom<SVGSVGElement, unknown>()
        .scaleExtent([0.3, 3])
        .on('zoom', event => container.attr('transform', event.transform))
    )

    svg.append('defs').append('marker')
      .attr('id', 'arrowhead')
      .attr('viewBox', '0 -5 10 10')
      .attr('refX', 28).attr('refY', 0)
      .attr('markerWidth', 6).attr('markerHeight', 6)
      .attr('orient', 'auto')
      .append('path').attr('d', 'M0,-5L10,0L0,5').attr('fill', '#444')

    const nodes: D3Node[] = graph.nodes.map(n => ({ ...n }))
    const links: D3Link[] = graph.edges.map(e => ({ ...e }))

    const simulation = d3.forceSimulation<D3Node>(nodes)
      .force('link',      d3.forceLink<D3Node, D3Link>(links).id(d => d.id).distance(130))
      .force('charge',    d3.forceManyBody().strength(-450))
      .force('center',    d3.forceCenter(width / 2, height / 2))
      .force('collision', d3.forceCollide(44))

    const link = container.append('g')
      .selectAll<SVGLineElement, D3Link>('line')
      .data(links).join('line')
      .attr('stroke', '#2a2a2a')
      .attr('stroke-width', 1.5)
      .attr('marker-end', 'url(#arrowhead)')

    const drag = d3.drag<SVGGElement, D3Node>()
      .on('start', (event, d) => { if (!event.active) simulation.alphaTarget(0.3).restart(); d.fx = d.x; d.fy = d.y })
      .on('drag',  (event, d) => { d.fx = event.x; d.fy = event.y })
      .on('end',   (event, d) => { if (!event.active) simulation.alphaTarget(0); d.fx = null; d.fy = null })

    const node = container.append('g')
      .selectAll<SVGGElement, D3Node>('g')
      .data(nodes).join('g')
      .attr('cursor', 'pointer')
      .on('click', (_, d) => setSelected(d))
      .call(drag)

    node.append('circle')
      .attr('r',            d => NODE_RADIUS[d.type] ?? 18)
      .attr('fill',         d => NODE_COLORS[d.type] ?? '#888')
      .attr('fill-opacity', 0.12)
      .attr('stroke',       d => NODE_COLORS[d.type] ?? '#888')
      .attr('stroke-width', 1.5)

    node.append('text')
      .attr('text-anchor', 'middle').attr('dy', '0.35em')
      .attr('fill', d => NODE_COLORS[d.type] ?? '#888')
      .attr('font-size', 8).attr('font-family', 'JetBrains Mono, monospace')
      .text(d => d.type)

    node.append('text')
      .attr('text-anchor', 'middle')
      .attr('dy', d => (NODE_RADIUS[d.type] ?? 18) + 14)
      .attr('fill', '#aaa').attr('font-size', 10)
      .attr('font-family', 'JetBrains Mono, monospace')
      .text(d => d.label.length > 22 ? d.label.substring(0, 20) + '…' : d.label)

    simulation.on('tick', () => {
      link
        .attr('x1', d => (d.source as D3Node).x ?? 0)
        .attr('y1', d => (d.source as D3Node).y ?? 0)
        .attr('x2', d => (d.target as D3Node).x ?? 0)
        .attr('y2', d => (d.target as D3Node).y ?? 0)
      node.attr('transform', d => `translate(${d.x ?? 0},${d.y ?? 0})`)
    })

    return () => { simulation.stop() }
  }, [graph])

  // ── Render ──────────────────────────────────────────────────────────────────

  return (
    <div>
      <p style={{ color: 'var(--text-muted)', fontSize: 'var(--font-size-sm)', marginBottom: 32 }}>
        Generate interactive lineage graphs — SOURCE → JOB → TARGET — from DataStage .dsx export files.
        Drag nodes, scroll to zoom.
      </p>

      {/* Load file */}
      <SectionLabel>load_datastage_xml</SectionLabel>
      <div className="card" style={{ marginBottom: 24 }}>
        <div style={{ display: 'flex', flexDirection: 'column', gap: 14 }}>

          {/* Drop zone */}
          <div
            onClick={() => fileInputRef.current?.click()}
            style={{
              border: `1px dashed ${fileName ? 'var(--accent-muted)' : 'var(--bg-border)'}`,
              borderRadius: 'var(--radius)',
              padding: '24px',
              textAlign: 'center',
              cursor: 'pointer',
              background: fileName ? 'var(--accent-bg)' : 'var(--bg-primary)',
              transition: 'all 0.15s',
            }}
          >
            <input
              ref={fileInputRef}
              type="file"
              accept=".dsx,.xml"
              onChange={handleFileChange}
              style={{ display: 'none' }}
            />
            {fileName ? (
              <div>
                <div style={{ color: 'var(--accent)', fontSize: 'var(--font-size-sm)', marginBottom: 4 }}>
                  ✓ {fileName}
                </div>
                <div style={{ color: 'var(--text-muted)', fontSize: 'var(--font-size-xs)' }}>
                  {(xmlSize / 1024).toFixed(1)} KB · click to change
                </div>
              </div>
            ) : (
              <div>
                <div style={{ color: 'var(--text-muted)', fontSize: 'var(--font-size-sm)', marginBottom: 4 }}>
                  click to upload .dsx / .xml
                </div>
                <div style={{ color: 'var(--text-muted)', fontSize: 'var(--font-size-xs)' }}>
                  no size limit
                </div>
              </div>
            )}
          </div>

          {error && (
            <div style={{ color: 'var(--status-error)', fontSize: 'var(--font-size-xs)' }}>
              ✗ {error}
            </div>
          )}

          <div style={{ display: 'flex', gap: 10 }}>
            <button
              className="btn btn--primary"
              onClick={generateLineage}
              disabled={loading || !fileName}
            >
              {loading ? '// parsing...' : '> generate_lineage'}
            </button>
          </div>

          <div style={{
            fontSize: 'var(--font-size-xs)', color: 'var(--text-muted)',
            padding: '8px 12px', background: 'var(--bg-primary)',
            border: '1px solid var(--bg-border)', borderLeft: '2px solid var(--accent-muted)',
            borderRadius: 'var(--radius)',
          }}>
            ℹ Upload the full <b style={{ color: 'var(--text-secondary)' }}>.dsx</b> export from DataStage.
            The graph maps all source tables, transformation stages, and target tables found in the job.
          </div>
        </div>
      </div>

      {/* Stats */}
      {graph && (
        <>
          <SectionLabel>graph_summary</SectionLabel>
          <div className="grid-4" style={{ marginBottom: 24 }}>
            <StatCard label="job_name"     value={graph.job_name} />
            <StatCard label="total_nodes"  value={graph.nodes.length} />
            <StatCard label="total_edges"  value={graph.edges.length} accent={graph.edges.length > 0} />
            <StatCard label="source_nodes" value={graph.nodes.filter(n => n.type === 'source').length} />
          </div>
        </>
      )}

      {/* Legend */}
      <div style={{ display: 'flex', gap: 24, marginBottom: 16 }}>
        {Object.entries(NODE_COLORS).map(([type, color]) => (
          <div key={type} style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
            <div style={{ width: 10, height: 10, borderRadius: '50%', background: color, boxShadow: `0 0 6px ${color}` }} />
            <span style={{ fontSize: 'var(--font-size-xs)', color: 'var(--text-secondary)' }}>{type}</span>
          </div>
        ))}
      </div>

      {/* Graph + detail panel */}
      {!graph ? (
        <EmptyState message="load a .dsx / .xml file and click generate_lineage to see the graph" />
      ) : (
        <div style={{ display: 'grid', gridTemplateColumns: '1fr 240px', gap: 16, alignItems: 'start' }}>

          {/* SVG canvas */}
          <div className="card" style={{ padding: 0, overflow: 'hidden' }}>
            <div style={{
              padding: '10px 16px',
              borderBottom: '1px solid var(--bg-border)',
              display: 'flex', justifyContent: 'space-between', alignItems: 'center',
            }}>
              <SectionLabel>{graph.job_name}</SectionLabel>
              <span style={{ fontSize: 'var(--font-size-xs)', color: 'var(--text-muted)' }}>
                {graph.nodes.length} nodes · {graph.edges.length} edges
              </span>
            </div>
            <svg
              ref={svgRef}
              width="100%"
              height={480}
              style={{ background: 'var(--bg-primary)', display: 'block' }}
            />
          </div>

          {/* Node detail */}
          <div className="card">
            <SectionLabel>node_detail</SectionLabel>
            {selected ? (
              <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
                <div>
                  <div style={{ fontSize: 'var(--font-size-xs)', color: 'var(--text-muted)' }}>id</div>
                  <div style={{ fontSize: 'var(--font-size-sm)', color: 'var(--accent-primary)', wordBreak: 'break-all' }}>{selected.id}</div>
                </div>
                <div>
                  <div style={{ fontSize: 'var(--font-size-xs)', color: 'var(--text-muted)' }}>label</div>
                  <div style={{ fontSize: 'var(--font-size-sm)', wordBreak: 'break-all' }}>{selected.label}</div>
                </div>
                <div>
                  <div style={{ fontSize: 'var(--font-size-xs)', color: 'var(--text-muted)' }}>type</div>
                  <div style={{ color: NODE_COLORS[selected.type], fontSize: 'var(--font-size-sm)' }}>● {selected.type}</div>
                </div>
                {selected.schema && (
                  <div>
                    <div style={{ fontSize: 'var(--font-size-xs)', color: 'var(--text-muted)' }}>schema</div>
                    <div style={{ fontSize: 'var(--font-size-sm)' }}>{selected.schema}</div>
                  </div>
                )}
                {selected.extra && Object.keys(selected.extra).length > 0 && (
                  <div>
                    <div style={{ fontSize: 'var(--font-size-xs)', color: 'var(--text-muted)', marginBottom: 4 }}>extra</div>
                    <pre style={{ fontSize: 9, color: 'var(--text-secondary)', whiteSpace: 'pre-wrap', wordBreak: 'break-all' }}>
                      {JSON.stringify(selected.extra, null, 2)}
                    </pre>
                  </div>
                )}
              </div>
            ) : (
              <div style={{ color: 'var(--text-muted)', fontSize: 'var(--font-size-xs)' }}>
                click a node to inspect it
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  )
}
