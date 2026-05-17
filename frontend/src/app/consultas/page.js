import { Suspense } from "react";
import ConsultasClient from "./ConsultasClient";

export const metadata = {
  title: "GeoContext | Consultas",
  description: "Consultas guiadas para explorar depositos, minerales y proximidad espacial.",
};

export default function ConsultasPage() {
  return (
    <Suspense fallback={null}>
      <ConsultasClient />
    </Suspense>
  );
}
