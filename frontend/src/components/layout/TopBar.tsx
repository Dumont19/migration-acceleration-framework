'use client'

import { usePathname } from 'next/navigation'
import styles from './TopBar.module.css'

const ROUTE_LABELS: Record<string, string> = {
  '/':           '// dashboard — system overview',
  '/migration':  '// migration — partitioned oracle → snowflake',
  '/dblink':     '// db_link — direct oracle → snowflake',
  '/gaps':       '// gap_analysis — volumetric comparison',
  '/docs':       '// job_docs — datastage documentation',
  '/lineage':    '// lineage — source → job → target graph',
  '/validation': '// validation — oracle vs snowflake comparison',
  '/logs':       '// audit_logs — persistent execution history',
  '/settings':   '// settings — connection configuration',
}

export function TopBar() {
  const pathname = usePathname()
  // Match most specific route first
  const label = Object.entries(ROUTE_LABELS)
    .sort((a, b) => b[0].length - a[0].length)
    .find(([route]) => pathname === route || pathname.startsWith(route + '/'))?.[1]
    ?? '// unknown_route'

  const now = new Date()
  const timestamp = now.toISOString().replace('T', ' ').substring(0, 19) + 'Z'

  return (
    <header className={styles.topbar}>
      <span className={styles.path}>{label}</span>
      <div className={styles.right}>
        <span className={styles.timestamp}>{timestamp}</span>
        <span className={styles.rec}>● REC</span>
      </div>
    </header>
  )
}
