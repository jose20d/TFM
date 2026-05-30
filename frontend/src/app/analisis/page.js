import { Suspense } from "react";
import AnalysisClient from "./AnalysisClient";

export const metadata = {
  title: "GeoContext | Analysis",
  description: "Global pattern analysis across economic, institutional, and deposit indicators.",
};

export default function AnalisisPage() {
  return (
    <Suspense fallback={null}>
      <AnalysisClient />
    </Suspense>
  );
}
