"use client";

import { usePathname, useRouter, useSearchParams } from "next/navigation";

export default function HomeCountryForm({
  lang,
  selectedLabel,
  placeholder,
  buttonText,
  options,
}) {
  const router = useRouter();
  const pathname = usePathname();
  const searchParams = useSearchParams();

  const onSubmit = (event) => {
    event.preventDefault();
    const formData = new FormData(event.currentTarget);
    const country = String(formData.get("pais") || "").trim();
    const nextLang = String(formData.get("lang") || lang || "es").trim() || "es";

    const params = new URLSearchParams(searchParams?.toString() || "");
    params.set("lang", nextLang);
    if (country) {
      params.set("pais", country);
    } else {
      params.delete("pais");
    }

    const query = params.toString();
    const href = query ? `${pathname}?${query}` : pathname;
    router.replace(href, { scroll: false });
  };

  return (
    <>
      <form className="country-form" method="get" onSubmit={onSubmit}>
        <input type="hidden" name="lang" value={lang} />
        <input
          name="pais"
          list="countries-options"
          defaultValue={selectedLabel}
          placeholder={placeholder}
        />
        <button type="submit">{buttonText}</button>
      </form>
      <datalist id="countries-options">
        {options.map((label) => (
          <option key={label} value={label} />
        ))}
      </datalist>
    </>
  );
}
