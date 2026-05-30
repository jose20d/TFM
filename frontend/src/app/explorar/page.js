import { Suspense } from "react";
import ExploreClient from "./ExploreClient";

export const metadata = {
  title: "GeoContext | Explore",
  description: "Geoterritorial exploration of mineral deposits and socioeconomic context.",
};

export default function ExplorarPage() {
  return (
    <Suspense fallback={null}>
      <ExploreClient />
    </Suspense>
  );
}
