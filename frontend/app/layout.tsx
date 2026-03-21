import './globals.css';  // ← AJOUTE CETTE LIGNE

export const metadata = {
  title: 'Intelligent Data Cleaner',
  description: 'AI-powered data cleaning assistant',
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="fr">
      <body className="antialiased">{children}</body>
    </html>
  );
}