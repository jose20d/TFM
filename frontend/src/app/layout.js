import { Inter } from "next/font/google";
import "./globals.css";
import VisitTracker from "../components/VisitTracker";

const inter = Inter({
  variable: "--font-inter",
  subsets: ["latin"],
});

export const metadata = {
  title: "GeoContext | Plataforma Analitica",
  description: "Panel analitico de datos mineros y variables socioeconomicas.",
};

export default function RootLayout({ children }) {
  return (
    <html lang="es" className={`${inter.variable} h-full antialiased`}>
      <body className="min-h-full flex flex-col">
        <VisitTracker />
        {children}
      </body>
    </html>
  );
}
