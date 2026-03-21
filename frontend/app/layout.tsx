import './globals.css';  // ← AJOUTE CETTE LIGNE

export const metadata = {
  title: 'Intelligent Data Cleaner',
  description: 'AI-powered data quality analysis',
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en">
      <body className="min-h-screen">{children}</body>
    </html>
  );
}