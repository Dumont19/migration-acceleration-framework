'use client'

import Link from 'next/link'
import { usePathname } from 'next/navigation'
import styles from './Sidebar.module.css'

const NAV_ITEMS = [
  { href: '/',           label: 'dashboard',  prefix: '/00' },
  { href: '/migration',  label: 'migration',  prefix: '/01' },
  { href: '/dblink',     label: 'db_link',    prefix: '/02' },
  { href: '/gaps',       label: 'gap_analysis', prefix: '/03' },
  { href: '/docs',       label: 'job_docs',   prefix: '/04' },
  { href: '/lineage',    label: 'lineage',    prefix: '/05' },
  { href: '/validation', label: 'validation', prefix: '/06' },
  { href: '/logs',       label: 'audit_logs', prefix: '/07' },
  { href: '/settings',   label: 'settings',   prefix: '/08' },
]

export function Sidebar() {
  const pathname = usePathname()

  return (
    <aside className={styles.sidebar}>
      {/* Logo / Brand */}
      <div className={styles.brand}>
        <span className={styles.brandAccent}>●</span>
        <div>
          <div className={styles.brandTitle}>MAF</div>
          <div className={styles.brandSub}>v4.0.0 // active</div>
        </div>
      </div>

      {/* Nav */}
      <nav className={styles.nav}>
        <div className={styles.navLabel}>// navigation</div>
        {NAV_ITEMS.map((item) => {
          const isActive = item.href === '/'
            ? pathname === '/'
            : pathname.startsWith(item.href)
          return (
            <Link
              key={item.href}
              href={item.href}
              className={`${styles.navItem} ${isActive ? styles.navItemActive : ''}`}
            >
              <span className={styles.navPrefix}>{item.prefix}</span>
              <span className={styles.navLabel2}>{item.label}</span>
              {isActive && <span className={styles.navIndicator} />}
            </Link>
          )
        })}
      </nav>

      {/* Footer */}
      <div className={styles.footer}>
        <div className={styles.footerItem}>
          <span className={styles.statusDotOk} />
          <span>oracle</span>
        </div>
        <div className={styles.footerItem}>
          <span className={styles.statusDotOk} />
          <span>snowflake</span>
        </div>
      </div>
    </aside>
  )
}
