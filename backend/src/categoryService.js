const fs = require("fs");
const path = require("path");
const axios = require("axios");
const ProxyAgent = require("proxy-agent");
const puppeteer = require("puppeteer-extra");
const StealthPlugin = require("puppeteer-extra-plugin-stealth");
const proxyChain = require("proxy-chain");
const { buildProxyUrl, buildProxyServer, buildPuppeteerProxyUrl } = require("./cookieUtils");

puppeteer.use(StealthPlugin());

const dataDir = path.join(__dirname, "..", "data");
const categoriesPath = path.join(dataDir, "categories.json");
const CATEGORY_CACHE_TTL_MS = 24 * 60 * 60 * 1000;
const CATEGORY_SOURCES = [
  "https://www.kleinanzeigen.de/s-kategorien.html"
];
const FALLBACK_CATEGORY_NAMES = [
  "Auto, Rad & Boot",
  "Immobilien",
  "Haus & Garten",
  "Mode & Beauty",
  "Elektronik",
  "Haustiere",
  "Familie, Kind & Baby",
  "Jobs",
  "Freizeit, Hobby & Nachbarschaft",
  "Musik, Filme & Bücher",
  "Eintrittskarten & Tickets",
  "Dienstleistungen",
  "Verschenken & Tauschen",
  "Unterricht & Kurse",
  "Nachbarschaftshilfe"
];

const DEBUG_CATEGORIES = process.env.KL_ENABLE_DEBUG === "1" && process.env.KL_DEBUG_CATEGORIES === "1";
const PUPPETEER_PROTOCOL_TIMEOUT = Number(process.env.PUPPETEER_PROTOCOL_TIMEOUT || 120000);
const PUPPETEER_LAUNCH_TIMEOUT = Number(process.env.PUPPETEER_LAUNCH_TIMEOUT || 120000);
const PUPPETEER_NAV_TIMEOUT = Number(process.env.PUPPETEER_NAV_TIMEOUT || 60000);

const buildAxiosConfig = ({ proxy, headers = {}, timeout = 20000 } = {}) => {
  const config = {
    headers,
    timeout,
    validateStatus: (status) => status >= 200 && status < 400
  };
  const proxyUrl = buildProxyUrl(proxy);
  if (proxyUrl) {
    let agent = null;
    if (typeof ProxyAgent === "function") {
      try {
        agent = new ProxyAgent(proxyUrl);
      } catch (error) {
        agent = ProxyAgent(proxyUrl);
      }
    } else if (ProxyAgent && typeof ProxyAgent.ProxyAgent === "function") {
      agent = new ProxyAgent.ProxyAgent(proxyUrl);
    }
    if (agent) {
      config.httpAgent = agent;
      config.httpsAgent = agent;
      config.proxy = false;
    }
  }
  return config;
};

const slugify = (value) =>
  String(value || "")
    .toLowerCase()
    .replace(/&/g, "and")
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");

const normalizeName = (value) =>
  String(value || "")
    .replace(/\s+/g, " ")
    .trim();

const decodeHtmlEntities = (value) =>
  String(value || "")
    .replace(/&amp;/g, "&")
    .replace(/&quot;/g, "\"")
    .replace(/&#39;/g, "'")
    .replace(/&nbsp;/g, " ")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .trim();

const normalizeHref = (href) => {
  if (!href) return "";
  if (/^https?:\/\//i.test(href)) return href;
  return `https://www.kleinanzeigen.de${href.startsWith("/") ? "" : "/"}${href}`;
};

const stripTags = (value) => String(value || "").replace(/<[^>]*>/g, " ");

const parseCategoriesFromHtml = (html) => {
  if (!html) return [];
  const results = [];
  const topRegex = /<li class="l-container-row">[\s\S]*?<h2[^>]*>[\s\S]*?<a[^>]+href="([^"]*\/c\d+[^"]*)"[^>]*>([\s\S]*?)<\/a>[\s\S]*?<\/h2>[\s\S]*?<ul>([\s\S]*?)<\/ul>/gi;
  let topMatch = null;
  while ((topMatch = topRegex.exec(html)) !== null) {
    const topUrl = normalizeHref(topMatch[1]);
    const topName = normalizeName(decodeHtmlEntities(topMatch[2]));
    const topIdMatch = topUrl.match(/\/c(\d+)(?:\/|$)/);
    const topId = topIdMatch ? topIdMatch[1] : "";
    const childHtml = topMatch[3] || "";
    const children = [];
    const childRegex = /<a[^>]+href="([^"]*\/c\d+[^"]*)"[^>]*>([\s\S]*?)<\/a>/gi;
    let childMatch = null;
    while ((childMatch = childRegex.exec(childHtml)) !== null) {
      const childUrl = normalizeHref(childMatch[1]);
      const childName = normalizeName(decodeHtmlEntities(childMatch[2]));
      const childIdMatch = childUrl.match(/\/c(\d+)(?:\/|$)/);
      const childId = childIdMatch ? childIdMatch[1] : "";
      if (!childId || !childName) continue;
      children.push({
        id: childId,
        name: childName,
        url: childUrl,
        children: []
      });
    }
    if (!topId || !topName) continue;
    results.push({
      id: topId,
      name: topName,
      url: topUrl,
      children
    });
  }
  return results;
};

const buildStaticCategoryTree = () => {
  const makeNode = (name, children = []) => ({
    id: slugify(name),
    name,
    url: "",
    children: children.map((child) => (typeof child === "string" ? makeNode(child) : makeNode(child.name, child.children)))
  });

  return [
    makeNode("Auto, Rad & Boot", [
      {
        name: "Autos",
        children: ["Gebrauchtwagen", "Oldtimer", "Youngtimer", "Zubehör", "Weitere Autos"]
      },
      {
        name: "Autoteile & Reifen",
        children: [
          "Auto Hifi & Navigation",
          "Ersatz- & Reparaturteile",
          "Reifen & Felgen",
          "Tuning & Styling",
          "Werkzeug",
          "Weitere Autoteile"
        ]
      },
      {
        name: "Boote & Bootszubehör",
        children: ["Boote", "Motoren", "Zubehör", "Trailer", "Weitere Boote"]
      },
      {
        name: "Fahrräder & Zubehör",
        children: ["Fahrräder", "E-Bikes", "Fahrradteile", "Zubehör", "Weitere Fahrräder"]
      },
      {
        name: "Motorräder & Motorroller",
        children: ["Motorräder", "Roller", "Quads", "Cross/Enduro", "Weitere Motorräder"]
      },
      {
        name: "Motorradteile & Zubehör",
        children: ["Ersatzteile", "Bekleidung", "Helme", "Zubehör", "Weitere Motorradteile"]
      },
      {
        name: "Nutzfahrzeuge & Anhänger",
        children: ["Transporter", "Anhänger", "Traktoren", "Baumaschinen", "Weitere Nutzfahrzeuge"]
      },
      {
        name: "Reparaturen & Dienstleistungen",
        children: ["Werkstätten", "Gutachten", "Pflege & Aufbereitung", "Transport", "Weitere Services"]
      },
      {
        name: "Wohnwagen & -mobile",
        children: ["Wohnwagen", "Wohnmobile", "Zubehör", "Stellplätze", "Weitere Wohnwagen"]
      },
      {
        name: "Weiteres Auto, Rad & Boot",
        children: ["Sammlerfahrzeuge", "Sonstiges", "Weitere Angebote"]
      }
    ]),
    makeNode("Immobilien", [
      {
        name: "Eigentumswohnungen",
        children: ["Neubau", "Bestand", "Kapitalanlage", "Penthouse", "Weitere Eigentumswohnungen"]
      },
      {
        name: "Häuser zum Kauf",
        children: ["Einfamilienhaus", "Mehrfamilienhaus", "Reihenhaus", "Bungalow", "Weitere Häuser"]
      },
      {
        name: "Mietwohnungen",
        children: ["1 Zimmer", "2 Zimmer", "3 Zimmer", "4+ Zimmer", "Weitere Mietwohnungen"]
      },
      {
        name: "Häuser zur Miete",
        children: ["Einfamilienhaus", "Reihenhaus", "Doppelhaushälfte", "Bungalow", "Weitere Miet-Häuser"]
      },
      {
        name: "Ferienwohnungen",
        children: ["Inland", "Ausland", "Apartment", "Ferienhaus", "Weitere Ferienwohnungen"]
      },
      {
        name: "Gewerbeimmobilien",
        children: ["Büro", "Laden", "Halle/Lager", "Gastronomie", "Weitere Gewerbeimmobilien"]
      },
      {
        name: "Grundstücke",
        children: ["Baugrundstücke", "Landwirtschaft", "Gewerbe", "Freizeit", "Weitere Grundstücke"]
      },
      {
        name: "Immobilienservice",
        children: ["Makler", "Hausverwaltung", "Finanzierung", "Bewertung", "Weitere Services"]
      }
    ]),
    makeNode("Haus & Garten", [
      {
        name: "Möbel & Wohnen",
        children: ["Schlafzimmer", "Wohnzimmer", "Esszimmer", "Büro", "Weitere Möbel"]
      },
      {
        name: "Haushalt",
        children: ["Küchenzubehör", "Reinigung", "Wäsche", "Bad", "Weitere Haushalt"]
      },
      {
        name: "Garten & Pflanzen",
        children: ["Gartenmöbel", "Pflanzen", "Teich", "Gartenwerkzeuge", "Weitere Garten"]
      },
      {
        name: "Heimwerken",
        children: ["Baumaterial", "Werkzeug", "Maschinen", "Sanitär", "Weitere Heimwerken"]
      },
      {
        name: "Dekoration",
        children: ["Bilder & Rahmen", "Kerzen & Lampen", "Textilien", "Vasen", "Weitere Deko"]
      },
      {
        name: "Küchen",
        children: ["Einbauküchen", "Küchengeräte", "Spülen", "Arbeitsplatten", "Weitere Küchen"]
      },
      {
        name: "Lampen & Licht",
        children: ["Deckenlampen", "Stehlampen", "Tischlampen", "Außenleuchten", "Weitere Lampen"]
      },
      {
        name: "Weitere Haus & Garten",
        children: ["Sonstiges", "Weitere Angebote"]
      }
    ]),
    makeNode("Mode & Beauty", [
      {
        name: "Damenbekleidung",
        children: ["Kleider", "Jacken", "Hosen", "Pullover", "Weitere Damenmode"]
      },
      {
        name: "Herrenbekleidung",
        children: ["Jacken", "Hosen", "Hemden", "Anzüge", "Weitere Herrenmode"]
      },
      {
        name: "Schuhe",
        children: ["Damenschuhe", "Herrenschuhe", "Kinderschuhe", "Sportschuhe", "Weitere Schuhe"]
      },
      {
        name: "Taschen & Accessoires",
        children: ["Handtaschen", "Rucksäcke", "Geldbörsen", "Gürtel", "Weitere Accessoires"]
      },
      {
        name: "Schmuck",
        children: ["Ketten", "Ringe", "Ohrringe", "Armbänder", "Weiterer Schmuck"]
      },
      {
        name: "Beauty & Pflege",
        children: ["Kosmetik", "Parfum", "Haarpflege", "Nagelpflege", "Weitere Beauty"]
      },
      {
        name: "Uhren",
        children: ["Damenuhren", "Herrenuhren", "Smartwatches", "Vintage", "Weitere Uhren"]
      },
      {
        name: "Weitere Mode & Beauty",
        children: ["Sonstiges", "Weitere Angebote"]
      }
    ]),
    makeNode("Elektronik", [
      {
        name: "Handy & Telefon",
        children: ["Smartphones", "Handys ohne Vertrag", "Festnetz", "Zubehör", "Weitere Telefone"]
      },
      {
        name: "Computer & Zubehör",
        children: ["Laptops", "PCs", "Monitore", "Drucker", "Weiteres Computerzubehör"]
      },
      {
        name: "TV & Audio",
        children: ["Fernseher", "Hi-Fi", "Lautsprecher", "Receiver", "Weitere TV & Audio"]
      },
      {
        name: "Foto",
        children: ["Kameras", "Objektive", "Zubehör", "Drohnen", "Weitere Foto"]
      },
      {
        name: "Haushaltsgeräte",
        children: ["Kühlschrank", "Waschmaschine", "Spülmaschine", "Kleingeräte", "Weitere Geräte"]
      },
      {
        name: "Konsolen",
        children: ["PlayStation", "Xbox", "Nintendo", "Zubehör", "Weitere Konsolen"]
      },
      {
        name: "Musik & DJ-Equipment",
        children: ["Instrumente", "DJ-Controller", "Studio", "PA", "Weitere Musik"]
      },
      {
        name: "Weitere Elektronik",
        children: ["Sonstiges", "Weitere Angebote"]
      }
    ]),
    makeNode("Haustiere", [
      {
        name: "Hunde",
        children: ["Welpen", "Zubehör", "Pflege", "Training", "Weitere Hunde"]
      },
      {
        name: "Katzen",
        children: ["Katzenzubehör", "Pflege", "Katzenmöbel", "Futter", "Weitere Katzen"]
      },
      {
        name: "Kleintiere",
        children: ["Kaninchen", "Meerschweinchen", "Hamster", "Zubehör", "Weitere Kleintiere"]
      },
      {
        name: "Vögel",
        children: ["Papageien", "Sittiche", "Zubehör", "Futter", "Weitere Vögel"]
      },
      {
        name: "Fische",
        children: ["Aquarien", "Zubehör", "Futter", "Teichfische", "Weitere Fische"]
      },
      {
        name: "Reptilien",
        children: ["Terrarien", "Zubehör", "Futter", "Sonstige Reptilien", "Weitere Reptilien"]
      },
      {
        name: "Tierbedarf",
        children: ["Futter", "Zubehör", "Pflege", "Transport", "Weiterer Tierbedarf"]
      },
      {
        name: "Tierbetreuung",
        children: ["Gassi-Service", "Tierpension", "Sitter", "Pflege", "Weitere Betreuung"]
      }
    ]),
    makeNode("Familie, Kind & Baby", [
      {
        name: "Baby",
        children: ["Kleidung", "Pflege", "Möbel", "Spielzeug", "Weitere Babyartikel"]
      },
      {
        name: "Kinderbekleidung",
        children: ["Mädchen", "Jungen", "Schuhe", "Jacken", "Weitere Kinderbekleidung"]
      },
      {
        name: "Spielzeug",
        children: ["Puppen", "Bauklötze", "Lego", "Brettspiele", "Weiteres Spielzeug"]
      },
      {
        name: "Kinderzimmer",
        children: ["Betten", "Schränke", "Schreibtische", "Deko", "Weitere Kinderzimmer"]
      },
      {
        name: "Kinderwagen",
        children: ["Kinderwagen", "Buggys", "Tragen", "Zubehör", "Weitere Kinderwagen"]
      },
      {
        name: "Schule",
        children: ["Schulranzen", "Bücher", "Lernmaterial", "Taschen", "Weitere Schule"]
      },
      {
        name: "Weitere Familie",
        children: ["Sonstiges", "Weitere Angebote"]
      }
    ]),
    makeNode("Jobs", [
      {
        name: "Vollzeit",
        children: ["Büro", "Verkauf", "Handwerk", "Logistik", "Weitere Vollzeitjobs"]
      },
      {
        name: "Teilzeit",
        children: ["Büro", "Verkauf", "Pflege", "Gastro", "Weitere Teilzeitjobs"]
      },
      {
        name: "Minijobs",
        children: ["Aushilfe", "Gastro", "Lager", "Reinigung", "Weitere Minijobs"]
      },
      {
        name: "Ausbildung",
        children: ["Kaufmann", "Handwerk", "IT", "Gesundheit", "Weitere Ausbildung"]
      },
      {
        name: "Praktika",
        children: ["Schüler", "Studenten", "Marketing", "IT", "Weitere Praktika"]
      },
      {
        name: "Nebenjob",
        children: ["Home Office", "Lieferdienst", "Promotion", "Nachhilfe", "Weitere Nebenjobs"]
      },
      {
        name: "Home Office",
        children: ["Kundenservice", "Texte", "Vertrieb", "IT", "Weitere Home Office"]
      }
    ]),
    makeNode("Freizeit, Hobby & Nachbarschaft", [
      {
        name: "Sport & Fitness",
        children: ["Fitnessgeräte", "Teamsport", "Laufsport", "Wassersport", "Weitere Sportartikel"]
      },
      {
        name: "Camping & Outdoor",
        children: ["Zelte", "Schlafsäcke", "Rucksäcke", "Kocher", "Weitere Outdoorartikel"]
      },
      {
        name: "Heimwerken & Sammeln",
        children: ["Sammelkarten", "Münzen", "Modelle", "Werkzeug", "Weitere Sammlungen"]
      },
      {
        name: "Reise & Veranstaltungen",
        children: ["Urlaub", "Events", "Tickets", "Gutscheine", "Weitere Reisen"]
      },
      {
        name: "Modellbau",
        children: ["Modelleisenbahn", "Flugzeuge", "Autos", "Bausätze", "Weiterer Modellbau"]
      },
      {
        name: "Kunst & Antiquitäten",
        children: ["Gemälde", "Skulpturen", "Antike Möbel", "Sammlerstücke", "Weitere Kunst"]
      },
      {
        name: "Weitere Freizeit",
        children: ["Sonstiges", "Weitere Angebote"]
      }
    ]),
    makeNode("Musik, Filme & Bücher", [
      {
        name: "Bücher",
        children: ["Romane", "Sachbücher", "Kinderbücher", "Comics", "Weitere Bücher"]
      },
      {
        name: "Filme",
        children: ["DVD", "Blu-ray", "Boxen", "Serien", "Weitere Filme"]
      },
      {
        name: "Musik",
        children: ["CD", "Vinyl", "Instrumente", "Zubehör", "Weitere Musik"]
      },
      {
        name: "Videospiele",
        children: ["PC", "PlayStation", "Xbox", "Nintendo", "Weitere Spiele"]
      },
      {
        name: "Zeitschriften",
        children: ["Magazine", "Sammlungen", "Fachzeitschriften", "Hefte", "Weitere Zeitschriften"]
      },
      {
        name: "Noten",
        children: ["Klavier", "Gitarre", "Gesang", "Orchester", "Weitere Noten"]
      }
    ]),
    makeNode("Eintrittskarten & Tickets", [
      {
        name: "Konzerte",
        children: ["Rock", "Pop", "Klassik", "Festivals", "Weitere Konzerte"]
      },
      {
        name: "Sport",
        children: ["Fußball", "Motorsport", "Tennis", "Eishockey", "Weitere Sporttickets"]
      },
      {
        name: "Theater & Musical",
        children: ["Theater", "Musical", "Oper", "Kabarett", "Weitere Bühnen"]
      },
      {
        name: "Events",
        children: ["Messen", "Comedy", "Show", "Gala", "Weitere Events"]
      },
      {
        name: "Kino",
        children: ["Premieren", "Gutscheine", "Serien", "Weitere Kino"]
      }
    ]),
    makeNode("Dienstleistungen", [
      {
        name: "Haus & Garten",
        children: ["Reinigung", "Gartenpflege", "Umzug", "Hausmeister", "Weitere Dienste Haus & Garten"]
      },
      {
        name: "Auto & Transport",
        children: ["Transport", "Umzug", "Kfz-Service", "Lieferung", "Weitere Auto & Transport"]
      },
      {
        name: "Handwerk",
        children: ["Maler", "Elektrik", "Sanitär", "Bau", "Weitere Handwerk"]
      },
      {
        name: "Unterricht",
        children: ["Nachhilfe", "Sprachen", "Musik", "IT", "Weiterer Unterricht"]
      },
      {
        name: "Beauty",
        children: ["Friseur", "Kosmetik", "Nagelstudio", "Massage", "Weitere Beauty"]
      },
      {
        name: "IT & Telekom",
        children: ["Support", "Webdesign", "Netzwerk", "Reparatur", "Weitere IT"]
      },
      {
        name: "Weitere Dienstleistungen",
        children: ["Sonstiges", "Weitere Angebote"]
      }
    ]),
    makeNode("Verschenken & Tauschen", [
      {
        name: "Verschenken",
        children: ["Möbel", "Elektronik", "Kleidung", "Sonstiges", "Weitere Geschenke"]
      },
      {
        name: "Tauschen",
        children: ["Tausch gegen", "Suche", "Angebote", "Sonstiges", "Weitere Tauschangebote"]
      }
    ]),
    makeNode("Unterricht & Kurse", [
      {
        name: "Nachhilfe",
        children: ["Mathe", "Deutsch", "Englisch", "Naturwissenschaften", "Weitere Nachhilfe"]
      },
      {
        name: "Sprachen",
        children: ["Englisch", "Deutsch", "Spanisch", "Französisch", "Weitere Sprachen"]
      },
      {
        name: "Musikunterricht",
        children: ["Klavier", "Gitarre", "Gesang", "Schlagzeug", "Weiterer Musikunterricht"]
      },
      {
        name: "Sport",
        children: ["Yoga", "Fitness", "Kampfsport", "Tanzen", "Weitere Sportkurse"]
      },
      {
        name: "Kunst & Gestaltung",
        children: ["Malen", "Fotografie", "Design", "Handwerk", "Weitere Kunstkurse"]
      },
      {
        name: "Beruf & Karriere",
        children: ["Coaching", "Bewerbung", "IT", "Marketing", "Weitere Kurse"]
      }
    ]),
    makeNode("Nachbarschaftshilfe", [
      {
        name: "Haushaltshilfe",
        children: ["Reinigung", "Einkauf", "Wäsche", "Kochen", "Weitere Haushaltshilfe"]
      },
      {
        name: "Nachhilfe",
        children: ["Schule", "Sprachen", "Mathe", "Sonstiges", "Weitere Nachhilfe"]
      },
      {
        name: "Fahrdienste",
        children: ["Arztfahrten", "Begleitung", "Einkauf", "Sonstige Fahrdienste", "Weitere Fahrdienste"]
      },
      {
        name: "Begleitung",
        children: ["Spaziergänge", "Behördengänge", "Arztbegleitung", "Freizeit", "Weitere Begleitung"]
      },
      {
        name: "Sonstige Hilfe",
        children: ["Reparaturen", "Aufbauhilfe", "Garten", "Sonstiges", "Weitere Hilfe"]
      }
    ])
  ];
};

const ensureDataDir = () => {
  if (!fs.existsSync(dataDir)) {
    fs.mkdirSync(dataDir, { recursive: true });
  }
};

const readCache = () => {
  try {
    const raw = fs.readFileSync(categoriesPath, "utf8");
    return JSON.parse(raw);
  } catch (error) {
    return null;
  }
};

const writeCache = (payload) => {
  ensureDataDir();
  fs.writeFileSync(categoriesPath, JSON.stringify(payload, null, 2));
};

const isCacheFresh = (cache) => {
  if (!cache?.updatedAt) return false;
  const updatedAt = new Date(cache.updatedAt).getTime();
  return Number.isFinite(updatedAt) && Date.now() - updatedAt < CATEGORY_CACHE_TTL_MS;
};

const isCacheComplete = (cache) => {
  if (!Array.isArray(cache?.categories) || cache.categories.length < 8) return false;
  const hasNumericId = cache.categories.some((node) => /^\d+$/.test(String(node?.id || "")));
  return hasNumericId;
};

const parseCategoriesFromPage = async (page, url) => {
  await page.goto(url, { waitUntil: "domcontentloaded", timeout: 20000 });
  return await page.evaluate(() => {
    const findTree = (obj) => {
      if (!obj || typeof obj !== "object") return null;
      if (Array.isArray(obj)) {
        for (const item of obj) {
          const found = findTree(item);
          if (found) return found;
        }
        return null;
      }
      if (obj.categories && Array.isArray(obj.categories)) {
        return obj.categories;
      }
      if (obj.categoryTree && Array.isArray(obj.categoryTree)) {
        return obj.categoryTree;
      }
      if (obj.categoryHierarchy && Array.isArray(obj.categoryHierarchy)) {
        return obj.categoryHierarchy;
      }
      for (const value of Object.values(obj)) {
        const found = findTree(value);
        if (found) return found;
      }
      return null;
    };

    const extractFromState = () => {
      const stateCandidates = [
        window.__INITIAL_STATE__,
        window.__PRELOADED_STATE__,
        window.__NEXT_DATA__,
        window.__NUXT__
      ];
      for (const candidate of stateCandidates) {
        const found = findTree(candidate);
        if (found) return found;
      }
      return null;
    };

    const parseDom = () => {
      const normalizeText = (value) => (value || "").replace(/\s+/g, " ").trim();
      const preferred = Array.from(document.querySelectorAll("main ul.treelist")).filter((list) =>
        list.querySelector('a[href*="/c"]')
      );
      const lists = (preferred.length ? preferred : Array.from(document.querySelectorAll("ul")))
        .filter((list) => list.querySelector('a[href*="/c"]'));
      if (!lists.length) return null;
      const targetList = lists.sort((a, b) => b.querySelectorAll("a[href*='/c']").length - a.querySelectorAll("a[href*='/c']").length)[0];

      const parseList = (list) =>
        Array.from(list.children)
          .filter((child) => child.tagName.toLowerCase() === "li")
          .map((li) => {
            const link = li.querySelector("a[href*='/c']");
            const url = link ? link.href : "";
            const name = link ? normalizeText(link.textContent) : "";
            const match = url.match(/\/c(\d+)(?:\/|$)/);
            const id = match ? match[1] : "";
            const childList = li.querySelector("ul");
            return {
              id,
              name,
              url,
              children: childList ? parseList(childList) : []
            };
          })
          .filter((item) => item.id && item.name);

      return parseList(targetList);
    };

    return {
      stateTree: extractFromState(),
      domTree: parseDom()
    };
  });
};

const fetchCategoriesFromPage = async ({ proxy } = {}) => {
  if (!proxy) {
    return [];
  }
  try {
    const response = await axios.get(
      "https://www.kleinanzeigen.de/s-kategorien.html",
      buildAxiosConfig({
        proxy,
        headers: {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept-Language": "de-DE,de;q=0.9,en;q=0.8"
        },
        timeout: 20000
      })
    );
    const htmlCategories = parseCategoriesFromHtml(response.data);
    if (htmlCategories.length) {
      return htmlCategories;
    }
  } catch (error) {
    // fallback to puppeteer below
  }

  const proxyServer = buildProxyServer(proxy);
  const proxyUrl = buildPuppeteerProxyUrl(proxy);
  if (!proxyServer || !proxyUrl) {
    return [];
  }
  const needsProxyChain = Boolean(
    proxyUrl && ((proxy?.type || "").toLowerCase().startsWith("socks") || proxy?.username || proxy?.password)
  );
  let anonymizedProxyUrl = "";
  const launchArgs = ["--no-sandbox", "--disable-setuid-sandbox", "--lang=de-DE", "--disable-dev-shm-usage", "--no-zygote", "--disable-gpu"];
  if (proxyServer) {
    if (needsProxyChain) {
      anonymizedProxyUrl = await proxyChain.anonymizeProxy(proxyUrl);
      launchArgs.push(`--proxy-server=${anonymizedProxyUrl}`);
    } else {
      launchArgs.push(`--proxy-server=${proxyServer}`);
    }
  }

  const browser = await puppeteer.launch({
    headless: "new",
    args: launchArgs,
    timeout: PUPPETEER_LAUNCH_TIMEOUT,
    protocolTimeout: PUPPETEER_PROTOCOL_TIMEOUT
  });

  try {
    const page = await browser.newPage();
    page.setDefaultTimeout(PUPPETEER_NAV_TIMEOUT);
    page.setDefaultNavigationTimeout(PUPPETEER_NAV_TIMEOUT);
    if (!anonymizedProxyUrl && (proxy?.username || proxy?.password)) {
      await page.authenticate({
        username: proxy.username || "",
        password: proxy.password || ""
      });
    }
    await page.setExtraHTTPHeaders({ "Accept-Language": "de-DE,de;q=0.9,en;q=0.8" });

    let loaded = false;
    let result = null;
    for (const url of CATEGORY_SOURCES) {
      try {
        result = await parseCategoriesFromPage(page, url);
        loaded = true;
        break;
      } catch (error) {
        continue;
      }
    }

    if (!loaded) {
      throw new Error("Не удалось загрузить страницу категорий Kleinanzeigen");
    }

    const countNodes = (nodes) => {
      if (!Array.isArray(nodes)) return 0;
      return nodes.reduce((total, node) => {
        const children = node?.children || node?.subcategories || node?.categories || [];
        return total + 1 + countNodes(children);
      }, 0);
    };

    const primary = result.stateTree;
    const secondary = result.domTree;
    const categories = countNodes(primary) >= countNodes(secondary) ? primary : secondary;
    if (!categories) {
      throw new Error("Не удалось извлечь дерево категорий");
    }

    return Array.isArray(categories) ? categories : [];
  } finally {
    await browser.close();
    if (anonymizedProxyUrl) {
      await proxyChain.closeAnonymizedProxy(anonymizedProxyUrl, true);
    }
  }
};

const extractIdFromUrl = (url) => {
  if (!url) return "";
  const match = url.match(/\/c(\d+)(?:\/|$)/);
  if (match) return match[1];
  const trailing = url.match(/(\d+)(?:\/|$)/);
  return trailing ? trailing[1] : "";
};

const extractIdentifierFromUrl = (url, targetId = "") => {
  if (!url) return "";
  const normalized = normalizeHref(url);
  const numeric = extractIdFromUrl(normalized);
  const slugMatch = normalized.match(/\/s-[^\/]+\/([^\/]+)\/c\d+(?:[/?+]|$)/);
  if (slugMatch) {
    const slug = slugMatch[1];
    if (slug && slug !== numeric && slug !== String(targetId || "")) return slug;
  }
  const attrMatch = normalized.match(/\+[^/]+:([^+/?&#]+)/);
  if (attrMatch) {
    const attr = attrMatch[1];
    if (attr && attr !== numeric && attr !== String(targetId || "")) return attr;
  }
  return numeric;
};

const buildCategoryUrl = (id) => {
  if (!id || !/^\d+$/.test(id)) return "";
  return `https://www.kleinanzeigen.de/s-kategorie/c${id}`;
};

const extractBrowseboxLists = (html) => {
  if (!html) return [];
  const lists = [];
  const openRegex = /<ul[^>]*class="[^"]*browsebox-itemlist[^"]*"[^>]*>/gi;
  let match = null;
  while ((match = openRegex.exec(html)) !== null) {
    const start = match.index;
    let cursor = match.index + match[0].length;
    let depth = 1;
    while (cursor < html.length) {
      const nextOpen = html.indexOf("<ul", cursor);
      const nextClose = html.indexOf("</ul", cursor);
      if (nextClose === -1) break;
      if (nextOpen !== -1 && nextOpen < nextClose) {
        depth += 1;
        cursor = nextOpen + 3;
        continue;
      }
      depth -= 1;
      cursor = nextClose + 4;
      if (depth === 0) {
        const end = html.indexOf(">", nextClose);
        if (end !== -1) {
          lists.push(html.slice(start, end + 1));
          openRegex.lastIndex = end + 1;
        }
        break;
      }
    }
  }
  return lists;
};

const parseCategoryLinksFromBlock = (blockHtml, targetId = "") => {
  if (!blockHtml) return [];
  const results = [];
  const seen = new Set();
  const linkRegex = /<a([^>]+)href=['"]([^'"]*\/c\d+[^'"]*)['"]([^>]*)>([\s\S]*?)<\/a>/gi;
  let match = null;
  while ((match = linkRegex.exec(blockHtml)) !== null) {
    const attrs = `${match[1] || ""} ${match[3] || ""}`;
    if (/icon-close/i.test(attrs) || /entfernen/i.test(attrs)) continue;
    const url = normalizeHref(match[2]);
    const id = extractIdentifierFromUrl(url, targetId);
    if (!id) continue;
    if (targetId && id === targetId) continue;
    if (seen.has(id)) continue;
    const name = normalizeName(decodeHtmlEntities(stripTags(match[4])));
    if (!name || /alle kategorien/i.test(name)) continue;
    seen.add(id);
    results.push({
      id,
      name,
      url,
      children: []
    });
  }
  return results;
};

const extractCategoryChildrenFromListingHtml = (html, targetId = "") => {
  if (!html) return [];
  const sectionMatch = html.match(/<section[^>]*>[\s\S]*?<h3[^>]*>\s*Kategorien\s*<\/h3>[\s\S]*?<\/section>/i);
  const sectionHtml = sectionMatch ? sectionMatch[0] : html;
  const lists = extractBrowseboxLists(sectionHtml);
  if (!lists.length) {
    const fallback = parseCategoryLinksFromBlock(sectionHtml, targetId);
    return fallback.length ? fallback : parseCategoryLinksFromBlock(html, targetId);
  }
  const targetToken = targetId ? `/c${targetId}` : "";
  let best = null;
  let bestScore = -1;
  lists.forEach((listHtml) => {
    const children = parseCategoryLinksFromBlock(listHtml, targetId);
    if (!children.length) return;
    let score = 0;
    if (targetToken) {
      score = children.reduce((total, child) => {
        return total + (child.url && child.url.includes(targetToken) ? 1 : 0);
      }, 0);
    }
    if (score > bestScore) {
      bestScore = score;
      best = children;
    } else if (best === null) {
      best = children;
    }
  });
  if (best && best.length) return best;
  const sectionFallback = parseCategoryLinksFromBlock(sectionHtml, targetId);
  if (sectionFallback.length) return sectionFallback;
  const pageFallback = parseCategoryLinksFromBlock(html, targetId);
  if (pageFallback.length) return pageFallback;
  if (targetId) {
    const idRegex = new RegExp(`<a[^>]+href=['"]([^'"]*c${targetId}[^'"]*)['"][^>]*>([\\s\\S]*?)<\\/a>`, "gi");
    const seen = new Set();
    const results = [];
    let match = null;
    while ((match = idRegex.exec(html)) !== null) {
      const href = match[1] || "";
      if (!/\+/.test(href) && !/\/s-[^\/]+\/[^\/]+\/c\d+/i.test(href)) continue;
      const name = normalizeName(decodeHtmlEntities(stripTags(match[2] || "")));
      if (!name || /alle kategorien/i.test(name)) continue;
      const url = normalizeHref(href);
      const id = extractIdentifierFromUrl(url, targetId);
      if (!id || seen.has(id)) continue;
      seen.add(id);
      results.push({ id, name, url, children: [] });
    }
    if (results.length) return results;
  }
  const lastList = lists[lists.length - 1];
  return parseCategoryLinksFromBlock(lastList, targetId);
};

const fetchCategoryChildrenFromListing = async ({ id, url, proxy } = {}) => {
  if (!proxy) return [];
  let targetId = id && /^\d+$/.test(String(id)) ? String(id) : "";
  const targetUrl = url ? normalizeHref(url) : (targetId ? buildCategoryUrl(targetId) : "");
  if (!targetId && targetUrl) {
    targetId = extractIdFromUrl(targetUrl);
  }
  if (!targetUrl) return [];
  try {
    const response = await axios.get(
      targetUrl,
      buildAxiosConfig({
        proxy,
        headers: {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Accept-Language": "de-DE,de;q=0.9,en;q=0.8"
        },
        timeout: 8000
      })
    );
    if (!response?.data) return [];
    const children = extractCategoryChildrenFromListingHtml(String(response.data || ""), targetId);
    if (DEBUG_CATEGORIES) {
      const html = String(response.data || "");
      const hrefMatches = [];
      const hrefRegex = /href=['"]([^'"]+)['"]/gi;
      let match = null;
      while ((match = hrefRegex.exec(html)) !== null && hrefMatches.length < 5) {
        const href = match[1] || "";
        if (/\/c\d+/.test(href) || /audio_hifi|mp3_player|cd_player|lautsprecher|radio_receiver|stereoanlagen|sonstiges/i.test(href)) {
          hrefMatches.push(href);
        }
      }
      const sample = children.slice(0, 10).map((item) => `${item.id}:${item.name}`).join(", ");
      console.log(`[categories/children] listing parsed url=${targetUrl} targetId=${targetId} count=${children.length} sample=${sample}`);
      console.log(`[categories/children] listing href sample: ${hrefMatches.join(" | ")}`);
    }
    return children;
  } catch (error) {
    if (DEBUG_CATEGORIES) {
      console.log(`[categories/children] listing fetch failed url=${targetUrl} targetId=${targetId} error=${error.message}`);
    }
    return [];
  }
};

const normalizeCategoryTree = (nodes) => {
  if (!Array.isArray(nodes)) return [];
  return nodes.map((node) => ({
    id: node.id ? String(node.id) : extractIdFromUrl(node.url || "") || slugify(node.name || node.label || ""),
    name: normalizeName(node.name || node.label || ""),
    url: node.url || buildCategoryUrl(String(node.id || extractIdFromUrl(node.url || "") || "")),
    children: normalizeCategoryTree(node.children || node.subcategories || node.categories || [])
  })).filter((node) => node.name && node.id);
};

const buildFallbackCategories = () =>
  FALLBACK_CATEGORY_NAMES.map((name) => ({
    id: slugify(name),
    name,
    url: "",
    children: []
  }));

const fetchCategories = async ({ proxy } = {}) => {
  try {
    if (!proxy) {
      return buildStaticCategoryTree();
    }
    const rawCategories = await fetchCategoriesFromPage({ proxy });
    const normalized = normalizeCategoryTree(rawCategories);
    if (!normalized.length || normalized.length < 8) {
      throw new Error("Список категорий выглядит неполным");
    }
    return normalized;
  } catch (error) {
    return buildStaticCategoryTree();
  }
};

const getCategoryChildren = async ({ id, url, proxy } = {}) => {
  const targetId = id ? String(id) : "";
  const targetUrl = url ? String(url) : "";
  const targetIdFromUrl = !targetId && targetUrl ? extractIdFromUrl(targetUrl) : "";
  const normalizeUrl = (value) =>
    String(value || "")
      .replace(/^https?:\/\/www\.kleinanzeigen\.de/i, "")
      .replace(/\/$/, "");

  const matchesNode = (node) => {
    if (!node) return false;
    if (targetId && String(node.id) === targetId) return true;
    if (targetUrl) {
      const nodeUrl = normalizeUrl(node.url || "");
      return nodeUrl && nodeUrl === normalizeUrl(targetUrl);
    }
    return false;
  };

  const findNode = (nodes) => {
    for (const node of nodes || []) {
      if (matchesNode(node)) return node;
      if (node.children?.length) {
        const found = findNode(node.children);
        if (found) return found;
      }
    }
    return null;
  };

  try {
    let data = await getCategories({ forceRefresh: false, proxy });
    let node = findNode(data?.categories || []);
    if (!node && proxy) {
      data = await getCategories({ forceRefresh: true, proxy });
      node = findNode(data?.categories || []);
    }
    if (node && Array.isArray(node.children) && node.children.length) return node.children || [];
    const fallbackUrl = node?.url || targetUrl || (targetId ? buildCategoryUrl(targetId) : "");
    if (proxy) {
      const fetchedChildren = await fetchCategoryChildrenFromListing({
        id: targetId || targetIdFromUrl,
        url: fallbackUrl,
        proxy
      });
      if (fetchedChildren.length) return fetchedChildren;
    }
  } catch (error) {
    // ignore and fallback below
  }

  if (targetId && !/^\d+$/.test(targetId)) {
    const staticTree = buildStaticCategoryTree();
    const node = findNode(staticTree);
    return node?.children || [];
  }

  if ((targetId || targetUrl) && proxy) {
    const fallbackUrl = targetUrl || (targetId ? buildCategoryUrl(targetId) : "");
    const fetchedChildren = await fetchCategoryChildrenFromListing({
      id: targetId || targetIdFromUrl,
      url: fallbackUrl,
      proxy
    });
    if (fetchedChildren.length) return fetchedChildren;
  }

  return [];
};

const getCategories = async ({ forceRefresh = false, proxy } = {}) => {
  const cache = readCache();
  // Prefer stale local cache over expensive live refresh to keep category UI responsive.
  if (!forceRefresh && cache && Array.isArray(cache.categories) && cache.categories.length) {
    return cache;
  }
  if (!forceRefresh && cache && isCacheFresh(cache) && isCacheComplete(cache)) {
    return cache;
  }

  if (!proxy) {
    if (cache && Array.isArray(cache.categories) && cache.categories.length) {
      return cache;
    }
    return {
      updatedAt: new Date().toISOString(),
      categories: buildStaticCategoryTree()
    };
  }

  const categories = await fetchCategories({ proxy });
  const payload = {
    updatedAt: new Date().toISOString(),
    categories
  };

  writeCache(payload);
  return payload;
};

module.exports = {
  getCategories,
  getCategoryChildren
};
