import type { Metadata } from 'next'
import { ThemeProvider } from '@/context/ThemeContext'
import { Sidebar } from '@/components/layout/Sidebar'
import { TopBar } from '@/components/layout/TopBar'
import '@/styles/globals.css'

export const metadata: Metadata = {
  title: 'MAF — Migration Acceleration Framework',
  description: 'Oracle → Snowflake migration acceleration framework',
}

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en" data-theme="light" suppressHydrationWarning>
      <head>
        {/* Prevent flash: apply saved theme before first paint */}
        <script dangerouslySetInnerHTML={{ __html: `
          try {
            const t = localStorage.getItem('maf-theme');
            if (t === 'dark') document.documentElement.setAttribute('data-theme', 'dark');
          } catch(e) {}
        `}} />
      </head>
      <body suppressHydrationWarning>
        <ThemeProvider>
          <div className="app-layout">
            <Sidebar />
            <TopBar />
            <main className="app-main">
              {children}
            </main>
          </div>
        </ThemeProvider>
      </body>
    </html>
  )
}
