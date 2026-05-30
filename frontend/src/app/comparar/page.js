import { Suspense } from "react";
import CompareClient from "./CompareClient";

export const metadata = {
  title: "GeoContext | Compare",
  description: "Comparison of geological and socioeconomic indicators across countries.",
};

export default function CompararPage() {
  return (
    <Suspense fallback={null}>
      <CompareClient />
    </Suspense>
  );
}
