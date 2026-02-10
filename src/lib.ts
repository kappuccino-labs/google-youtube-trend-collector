import "dotenv/config";
import { Kafka, CompressionTypes } from "kafkajs";

// ── Environment ──────────────────────────────────────────────────────
export const KAFKA_BOOTSTRAP_SERVERS = process.env.KAFKA_BOOTSTRAP_SERVERS ?? "";
export const KAFKA_API_KEY = process.env.KAFKA_API_KEY ?? "";
export const KAFKA_API_SECRET = process.env.KAFKA_API_SECRET ?? "";
export const KAFKA_GROUP_ID = process.env.KAFKA_GROUP_ID ?? "mcp-consumer-group";

export const GOOGLE_API_KEY = process.env.GOOGLE_API_KEY ?? "";
export const GOOGLE_CSE_ID = process.env.GOOGLE_CSE_ID ?? "";
export const YOUTUBE_API_KEY = process.env.YOUTUBE_API_KEY ?? "";
export const NAVER_CLIENT_ID = process.env.NAVER_CLIENT_ID ?? "";
export const NAVER_CLIENT_SECRET = process.env.NAVER_CLIENT_SECRET ?? "";

export const GOOGLE_SEARCH_TOPIC = "google-search-results";
export const YOUTUBE_SEARCH_TOPIC = "youtube-search-results";
export const YOUTUBE_INGREDIENTS_TOPIC = "youtube-ingredient-prices";
export const NAVER_INGREDIENT_PRICES_TOPIC = "naver-ingredient-prices";

export { CompressionTypes };

// ── Kafka Client ─────────────────────────────────────────────────────
export const kafka = new Kafka({
  clientId: "google-youtube-trend-collector",
  brokers: KAFKA_BOOTSTRAP_SERVERS.split(","),
  ssl: true,
  sasl: {
    mechanism: "plain",
    username: KAFKA_API_KEY,
    password: KAFKA_API_SECRET,
  },
});

// ── Google Custom Search API ─────────────────────────────────────────
export interface GoogleSearchItem {
  title: string;
  link: string;
  snippet: string;
  displayLink: string;
  formattedUrl: string;
  pagemap?: {
    cse_thumbnail?: Array<{ src: string; width: string; height: string }>;
    metatags?: Array<Record<string, string>>;
  };
}

export interface GoogleSearchResult {
  searchInformation: {
    totalResults: string;
    searchTime: number;
    formattedTotalResults: string;
    formattedSearchTime: string;
  };
  items?: GoogleSearchItem[];
}

export async function searchGoogle(
  query: string,
  start = 1,
  num = 10,
  dateRestrict?: string,
): Promise<GoogleSearchResult> {
  if (!GOOGLE_API_KEY || !GOOGLE_CSE_ID) {
    throw new Error("GOOGLE_API_KEY와 GOOGLE_CSE_ID 환경변수를 설정해주세요.");
  }

  const url = new URL("https://www.googleapis.com/customsearch/v1");
  url.searchParams.set("key", GOOGLE_API_KEY);
  url.searchParams.set("cx", GOOGLE_CSE_ID);
  url.searchParams.set("q", query);
  url.searchParams.set("start", String(start));
  url.searchParams.set("num", String(num));
  url.searchParams.set("gl", "kr");
  url.searchParams.set("lr", "lang_ko");
  if (dateRestrict) url.searchParams.set("dateRestrict", dateRestrict);

  const response = await fetch(url.toString());

  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(`Google API 오류 (${response.status}): ${errorText}`);
  }

  return response.json() as Promise<GoogleSearchResult>;
}

// ── YouTube Data API v3 ──────────────────────────────────────────────
export interface YouTubeSearchItem {
  kind: string;
  id: { kind: string; videoId?: string; channelId?: string; playlistId?: string };
  snippet: {
    publishedAt: string;
    channelId: string;
    title: string;
    description: string;
    thumbnails: Record<string, { url: string; width: number; height: number }>;
    channelTitle: string;
    liveBroadcastContent: string;
    publishTime: string;
  };
}

export interface YouTubeSearchResult {
  kind: string;
  pageInfo: { totalResults: number; resultsPerPage: number };
  nextPageToken?: string;
  items: YouTubeSearchItem[];
}

export interface YouTubeVideoStats {
  id: string;
  statistics: {
    viewCount: string;
    likeCount: string;
    commentCount: string;
  };
}

export interface YouTubeVideoStatsResult {
  items: YouTubeVideoStats[];
}

export async function searchYouTube(
  query: string,
  maxResults = 25,
  order: "date" | "viewCount" | "relevance" = "date",
  publishedAfter?: string,
): Promise<YouTubeSearchResult> {
  if (!YOUTUBE_API_KEY) {
    throw new Error("YOUTUBE_API_KEY 환경변수를 설정해주세요.");
  }

  const url = new URL("https://www.googleapis.com/youtube/v3/search");
  url.searchParams.set("key", YOUTUBE_API_KEY);
  url.searchParams.set("part", "snippet");
  url.searchParams.set("q", query);
  url.searchParams.set("type", "video");
  url.searchParams.set("maxResults", String(maxResults));
  url.searchParams.set("order", order);
  url.searchParams.set("regionCode", "KR");
  url.searchParams.set("relevanceLanguage", "ko");
  if (publishedAfter) url.searchParams.set("publishedAfter", publishedAfter);

  const response = await fetch(url.toString());

  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(`YouTube Search API 오류 (${response.status}): ${errorText}`);
  }

  return response.json() as Promise<YouTubeSearchResult>;
}

export async function getYouTubeVideoStats(
  videoIds: string[],
): Promise<YouTubeVideoStatsResult> {
  if (!YOUTUBE_API_KEY) {
    throw new Error("YOUTUBE_API_KEY 환경변수를 설정해주세요.");
  }

  const url = new URL("https://www.googleapis.com/youtube/v3/videos");
  url.searchParams.set("key", YOUTUBE_API_KEY);
  url.searchParams.set("part", "statistics");
  url.searchParams.set("id", videoIds.join(","));

  const response = await fetch(url.toString());

  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(`YouTube Videos API 오류 (${response.status}): ${errorText}`);
  }

  return response.json() as Promise<YouTubeVideoStatsResult>;
}

// ── Naver Shopping Search API ────────────────────────────────────────
export interface NaverShopItem {
  title: string;
  link: string;
  image: string;
  lprice: string;
  hprice: string;
  mallName: string;
  productId: string;
  productType: string;
  brand: string;
  maker: string;
  category1: string;
  category2: string;
  category3: string;
  category4: string;
}

export interface NaverSearchResult<T = unknown> {
  lastBuildDate: string;
  total: number;
  start: number;
  display: number;
  items: T[];
}

export async function searchNaverShopping(
  query: string,
  display = 100,
  sort: "sim" | "date" | "asc" | "dsc" = "sim",
): Promise<NaverSearchResult<NaverShopItem>> {
  if (!NAVER_CLIENT_ID || !NAVER_CLIENT_SECRET) {
    throw new Error("NAVER_CLIENT_ID와 NAVER_CLIENT_SECRET 환경변수를 설정해주세요.");
  }

  const url = new URL("https://openapi.naver.com/v1/search/shop.json");
  url.searchParams.set("query", query);
  url.searchParams.set("display", String(display));
  url.searchParams.set("sort", sort);

  const response = await fetch(url.toString(), {
    headers: {
      "X-Naver-Client-Id": NAVER_CLIENT_ID,
      "X-Naver-Client-Secret": NAVER_CLIENT_SECRET,
    },
  });

  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(`Naver API 오류 (${response.status}): ${errorText}`);
  }

  return response.json() as Promise<NaverSearchResult<NaverShopItem>>;
}

export function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// 일주일 전 ISO 날짜
export function oneWeekAgo(): string {
  const d = new Date();
  d.setDate(d.getDate() - 7);
  return d.toISOString();
}
