import type { Viewport } from 'next';
import { IBM_Plex_Sans, IBM_Plex_Mono } from 'next/font/google';
import './globals.css';

// Deux familles d'une même maison, deux rôles. IBM Plex Sans porte
// l'interface — une fonte d'outil d'entreprise, faite pour les tableaux et
// les rapports ; IBM Plex Mono porte tout ce qui est un identifiant ou une
// mesure — un nom de
// colonne EST un identifiant de code, et il se lit mieux dans la fonte où
// il finira. Chargées par next/font : servies depuis le domaine, donc pas
// de requête vers Google au chargement et pas de saut de police.
const sans = IBM_Plex_Sans({
  subsets: ['latin'],
  weight: ['400', '500', '600', '700'],
  display: 'swap',
  variable: '--font-sans',
});

const mono = IBM_Plex_Mono({
  subsets: ['latin'],
  weight: ['400', '500', '600'],
  display: 'swap',
  variable: '--font-mono',
});

export const metadata = {
  title: 'Intelligent Data Cleaner — contrôle avant chargement en base',
  description:
    "Déclarez l'usage d'un CSV ou d'un Excel : l'outil en déduit un contrat, répare dans ce sens, vérifie indépendamment et dit ce qu'il a refusé d'inventer.",
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
