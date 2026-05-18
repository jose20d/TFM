import { Suspense } from "react";
import CompareClient from "./CompareClient";

export const metadata = {
  title: "GeoContext | Comparar",
  description: "Comparacion de indicadores geologicos y socioeconomicos entre paises.",
};

export default function CompararPage() {
  return (
    <Suspense fallback={null}>
      <CompareClient />
    </Suspense>
  );
}
