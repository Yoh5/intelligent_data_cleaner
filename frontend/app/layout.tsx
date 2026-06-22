import type { Viewport } from 'next';
import './globals.css';

export const metadata = {
  title: 'Intelligent Data Cleaner',
  description: 'AI-powered data cleaning assistant',
};

export const viewport: Viewport = {
  width: 'device-width',
  initialScale: 1,
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="fr">
      <body className="antialiased">{children}</body>
    </html>
  );
}
