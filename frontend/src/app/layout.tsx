import type { Metadata } from 'next'
import '../styles/globals.css'
import { Sidebar } from '@/components/layout/Sidebar'
import { TopBar } from '@/components/layout/TopBar'

export const metadata: Metadata = {
  title: 'MAF — Migration Acceleration Framework',
  description: 'Oracle → Snowflake migration orchestration platform',
}

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body>
        <div className="app-layout">
          <Sidebar />
          <TopBar />
          <main className="app-main">
            {children}
          </main>
        </div>
      </body>
    </html>
  )
}
