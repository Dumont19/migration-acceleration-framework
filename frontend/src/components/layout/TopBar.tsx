'use client'

import { useEffect, useState } from 'react'
import { usePathname } from 'next/navigation'
import { useTheme } from '@/context/ThemeContext'
import styles from './TopBar.module.css'

const ROUTE_LABELS: Record<string, string> = {
  '/':           '// dashboard — system overview',
  '/migration':  '// migration — partitioned oracle → snowflake',
  '/dblink':     '// db_link — direct oracle → snowflake',
  '/gaps':       '// gap_analysis — volumetric comparison',
  '/docs':       '// job_docs — datastage documentation',
  '/lineage':    '// lineage — source → job → target graph',
  '/validation': '// validation — oracle vs snowflake comparison',
  '/tools':      '// tools — metadata · create table · copy into · merge',
  '/logs':       '// audit_logs — persistent execution history',
  '/settings':   '// settings — connection configuration',
}

function MoonIcon() {
  return (
    <svg width="14" height="14" viewBox="0 0 24 24" fill="none"
      stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z"/>
    </svg>
  )
}

function SunIcon() {
  return (
    <svg width="14" height="14" viewBox="0 0 24 24" fill="none"
      stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <circle cx="12" cy="12" r="5"/>
      <line x1="12" y1="1"  x2="12" y2="3"/>
      <line x1="12" y1="21" x2="12" y2="23"/>
      <line x1="4.22" y1="4.22"  x2="5.64"  y2="5.64"/>
      <line x1="18.36" y1="18.36" x2="19.78" y2="19.78"/>
      <line x1="1"  y1="12" x2="3"  y2="12"/>
      <line x1="21" y1="12" x2="23" y2="12"/>
      <line x1="4.22" y1="19.78" x2="5.64"  y2="18.36"/>
      <line x1="18.36" y1="5.64"  x2="19.78" y2="4.22"/>
    </svg>
  )
}

export function TopBar() {
  const pathname  = usePathname()
  const { theme, toggleTheme } = useTheme()
  const [mounted, setMounted]   = useState(false)
  const [timestamp, setTimestamp] = useState('')

  useEffect(() => {
    setMounted(true)
    const update = () => {
      setTimestamp(
        new Date().toISOString().replace('T', ' ').substring(0, 19) + 'Z'
      )
    }
    update()
    const interval = setInterval(update, 1000)
    return () => clearInterval(interval)
  }, [])

  const label = Object.entries(ROUTE_LABELS)
    .sort((a, b) => b[0].length - a[0].length)
    .find(([route]) => pathname === route || pathname.startsWith(route + '/'))?.[1]
    ?? '// unknown_route'

  const isDark = theme === 'dark'

  return (
    <header className={styles.topbar}>
      <span className={styles.path}>{label}</span>
      <div className={styles.right}>
        <span className={styles.timestamp} suppressHydrationWarning>
          {mounted ? timestamp : ''}
        </span>

        {/* Theme toggle — pill with moon/sun + sliding thumb */}
        {mounted && (
          <button
            className={styles.themeToggle}
            onClick={toggleTheme}
            aria-label="Toggle theme"
          >
            <span className={styles.themeIconLeft}>
              {isDark ? <SunIcon /> : <MoonIcon />}
            </span>
            <div className={styles.toggleTrack}>
              <div className={`${styles.toggleThumb} ${isDark ? styles.thumbActive : ''}`} />
            </div>
          </button>
        )}

        <span className={styles.rec}>● REC</span>
      </div>
    </header>
  )
}
