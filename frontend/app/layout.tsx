import type { Viewport } from 'next';
import { Instrument_Sans, JetBrains_Mono } from 'next/font/google';
import './globals.css';

// Deux familles, deux rôles. Instrument Sans porte l'interface ; JetBrains
// Mono porte tout ce qui est un identifiant ou une mesure — un nom de
// colonne EST un identifiant de code, et il se lit mieux dans la fonte où
// il finira. Chargées par next/font : servies depuis le domaine, donc pas
// de requête vers Google au chargement et pas de saut de police.
const sans = Instrument_Sans({
  subsets: ['latin'],
  display: 'swap',
  variable: '--font-sans',
});

const mono = JetBrains_Mono({
  subsets: ['latin'],
  display: 'swap',
  variable: '--font-mono',
});

export const metadata = {
  title: 'Intelligent Data Cleaner',
  description:
    "Analyse un CSV ou un Excel, montre ce qui ne va pas colonne par colonne, et rend un script Python autonome que vous pouvez relire et rejouer.",
};

export const viewport: Viewport = {
  width: 'device-width',
  initialScale: 1,
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="fr" className={`${sans.variable} ${mono.variable}`}>
      <body className="antialiased font-sans">{children}</body>
    </html>
  );
}
