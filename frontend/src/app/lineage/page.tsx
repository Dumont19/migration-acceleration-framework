'use client'

import { useEffect, useRef, useState } from 'react'
import * as d3 from 'd3'
import { analysisApi } from '@/lib/api'
import type { LineageGraph, LineageNode, LineageEdge } from '@/lib/api'
import { SectionLabel, EmptyState } from '@/components/ui'

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

export default function LineagePage() {
  const svgRef = useRef<SVGSVGElement>(null)
  const [graph, setGraph] = useState<LineageGraph | null>(null)
  const [xmlInput, setXmlInput] = useState('')
  const [loading, setLoading] = useState(false)
  const [selected, setSelected] = useState<LineageNode | null>(null)
  const [error, setError] = useState<string | null>(null)

  const loadFromXml = async () => {
    if (!xmlInput.trim()) { setError('Paste DataStage XML content'); return }
    setError(null)
    setLoading(true)
    try {
      const data = await analysisApi.uploadDsxForLineage(xmlInput)
      setGraph(data)
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : 'Failed to parse XML')
    } finally {
      setLoading(false)
    }
  }

  const loadExample = async () => {
    setLoading(true)
    try {
      const data = await analysisApi.getLineage('example')
      setGraph(data)
    } catch {
      setError('No example data available — paste XML to generate lineage.')
    } finally {
      setLoading(false)
    }
  }

  // D3 force simulation
  useEffect(() => {
    if (!graph || !svgRef.current) return

    const svg = d3.select(svgRef.current)
    svg.selectAll('*').remove()

    const width = svgRef.current.clientWidth || 800
    const height = 480

    // Zoom container
    const container = svg.append('g')
    svg.call(
      d3.zoom<SVGSVGElement, unknown>()
        .scaleExtent([0.3, 3])
        .on('zoom', (event) => container.attr('transform', event.transform))
    )

    // Arrow marker
    svg.append('defs').append('marker')
      .attr('id', 'arrowhead')
      .attr('viewBox', '0 -5 10 10')
      .attr('refX', 28)
      .attr('refY', 0)
      .attr('markerWidth', 6)
      .attr('markerHeight', 6)
      .attr('orient', 'auto')
      .append('path')
      .attr('d', 'M0,-5L10,0L0,5')
      .attr('fill', '#555')

    type D3Node = LineageNode & d3.SimulationNodeDatum
    type D3Link = { source: string | D3Node; target: string | D3Node } & LineageEdge

    const nodes: D3Node[] = graph.nodes.map(n => ({ ...n }))
    const links: D3Link[] = graph.edges.map(e => ({ ...e }))

    const simulation = d3.forceSimulation<D3Node>(nodes)
      .force('link', d3.forceLink<D3Node, D3Link>(links).id(d => d.id).distance(120))
      .force('charge', d3.forceManyBody().strength(-400))
      .force('center', d3.forceCenter(width / 2, height / 2))
      .force('collision', d3.forceCollide(40))

    // Links
    const link = container.append('g')
      .selectAll('line')
      .data(links)
      .join('line')
      .attr('stroke', '#333')
      .attr('stroke-width', 1.5)
      .attr('marker-end', 'url(#arrowhead)')

    // Nodes
    const node = container.append('g')
      .selectAll('g')
      .data(nodes)
      .join('g')
      .attr('cursor', 'pointer')
      .on('click', (_, d) => setSelected(d))
      .call(
        d3.drag<SVGGElement, D3Node>()
          .on('start', (event, d) => { if (!event.active) simulation.alphaTarget(0.3).restart(); d.fx = d.x; d.fy = d.y })
          .on('drag', (event, d) => { d.fx = event.x; d.fy = event.y })
          .on('end', (event, d) => { if (!event.active) simulation.alphaTarget(0); d.fx = null; d.fy = null })
      )

    node.append('circle')
      .attr('r', d => NODE_RADIUS[d.type] ?? 18)
      .attr('fill', d => NODE_COLORS[d.type] ?? '#888')
      .attr('fill-opacity', 0.15)
      .attr('stroke', d => NODE_COLORS[d.type] ?? '#888')
      .attr('stroke-width', 1.5)

    node.append('text')
      .attr('text-anchor', 'middle')
      .attr('dy', d => (NODE_RADIUS[d.type] ?? 18) + 14)
      .attr('fill', '#ccc')
      .attr('font-size', 10)
      .attr('font-family', 'JetBrains Mono, monospace')
      .text(d => d.label.length > 22 ? d.label.substring(0, 20) + '…' : d.label)

    // Type label inside circle
    node.append('text')
      .attr('text-anchor', 'middle')
      .attr('dy', '0.35em')
      .attr('fill', d => NODE_COLORS[d.type] ?? '#888')
      .attr('font-size', 8)
      .attr('font-family', 'JetBrains Mono, monospace')
      .text(d => d.type)

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

  return (
    <div>
      <p style={{ color: 'var(--text-muted)', fontSize: 'var(--font-size-sm)', marginBottom: 32 }}>
        Interactive lineage graph — SOURCE → JOB → TARGET. Drag nodes, scroll to zoom.
      </p>

      {/* Legend */}
      <div style={{ display: 'flex', gap: 24, marginBottom: 24 }}>
        {Object.entries(NODE_COLORS).map(([type, color]) => (
          <div key={type} style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
            <div style={{ width: 10, height: 10, borderRadius: '50%', background: color, boxShadow: `0 0 6px ${color}` }} />
            <span style={{ fontSize: 'var(--font-size-xs)', color: 'var(--text-secondary)' }}>{type}</span>
          </div>
        ))}
      </div>

      {/* XML input */}
      <SectionLabel>load_datastage_xml</SectionLabel>
      <div className="card" style={{ marginBottom: 24 }}>
        <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
          <textarea
            className="form-input"
            rows={4}
            placeholder="Paste DataStage .dsx XML content here..."
            value={xmlInput}
            onChange={e => setXmlInput(e.target.value)}
            style={{ resize: 'vertical', fontFamily: 'var(--font-mono)', fontSize: 'var(--font-size-xs)' }}
          />
          {error && <div style={{ color: 'var(--status-error)', fontSize: 'var(--font-size-xs)' }}>✗ {error}</div>}
          <div style={{ display: 'flex', gap: 8 }}>
            <button className="btn btn--primary" onClick={loadFromXml} disabled={loading}>
              {loading ? '// parsing...' : '> generate_lineage'}
            </button>
            <button className="btn btn--ghost" onClick={loadExample} disabled={loading}>
              load example
            </button>
          </div>
        </div>
      </div>

      {/* Graph */}
      {!graph ? (
        <EmptyState message="paste DataStage XML and generate lineage to see the graph" />
      ) : (
        <div style={{ display: 'grid', gridTemplateColumns: '1fr 240px', gap: 16, alignItems: 'start' }}>
          <div className="card" style={{ padding: 0, overflow: 'hidden' }}>
            <div style={{ padding: '10px 16px', borderBottom: '1px solid var(--bg-border)', display: 'flex', justifyContent: 'space-between' }}>
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

          {/* Node detail panel */}
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
