import {
  kafka,
  searchGoogle,
  CompressionTypes,
  GOOGLE_SEARCH_TOPIC,
  sleep,
} from "./lib.js";

interface QueryGroup {
  category: string;
  keywords: string[];
}

const queries: QueryGroup[] = [
  {
    category: "두바이쿠키",
    keywords: [
      "두바이 초콜릿 쿠키",
      "두바이 쫀득 쿠키 레시피",
      "두바이 쿠키 재료 가격",
      "dubai chocolate cookie trend",
    ],
  },
  {
    category: "유행디저트",
    keywords: [
      "2025 유행 디저트 트렌드",
      "크럼블쿠키 인기",
      "약과 디저트 유행",
      "소금빵 트렌드",
      "휘낭시에 맛집",
    ],
  },
  {
    category: "유행음식",
    keywords: [
      "2025 음식 트렌드",
      "마라탕 인기",
      "로제떡볶이 트렌드",
      "제로음료 시장",
      "오마카세 트렌드",
    ],
  },
  {
    category: "유행카페",
    keywords: [
      "2025 핫플 카페 추천",
      "성수 카페 트렌드",
      "을지로 카페 핫플",
      "카페 디저트 트렌드",
    ],
  },
];

async function main() {
  // 토픽 생성
  const admin = kafka.admin();
  await admin.connect();
  const created = await admin.createTopics({
    topics: [{ topic: GOOGLE_SEARCH_TOPIC, numPartitions: 6, replicationFactor: 3 }],
  });
  console.log(`토픽 '${GOOGLE_SEARCH_TOPIC}':`, created ? "새로 생성" : "이미 존재");
  await admin.disconnect();

  const producer = kafka.producer();
  await producer.connect();

  let totalMessages = 0;

  for (const { category, keywords } of queries) {
    console.log(`\n${"=".repeat(60)}`);
    console.log(`📂 카테고리: ${category}`);
    console.log("=".repeat(60));

    for (const keyword of keywords) {
      try {
        // 최근 1주일 결과
        const data = await searchGoogle(keyword, 1, 10, "w1");
        const items = data.items || [];

        if (items.length === 0) {
          console.log(`  ⏭️  "${keyword}" - 결과 없음`);
          continue;
        }

        const messages = items.map((item, idx) => ({
          key: `google:${category}:${keyword}:${idx}`,
          value: JSON.stringify({
            type: "google_search",
            category,
            keyword,
            requestedAt: new Date().toISOString(),
            totalResults: data.searchInformation.totalResults,
            searchTime: data.searchInformation.searchTime,
            item: {
              title: item.title,
              link: item.link,
              snippet: item.snippet,
              displayLink: item.displayLink,
            },
          }),
          headers: {
            source: "google-search-collector",
            category,
            query: keyword,
          },
        }));

        await producer.send({
          topic: GOOGLE_SEARCH_TOPIC,
          compression: CompressionTypes.GZIP,
          messages,
        });

        totalMessages += messages.length;
        console.log(
          `  ✅ "${keyword}" - ${items.length}건 (전체 약 ${parseInt(data.searchInformation.totalResults).toLocaleString()}건)`,
        );
        await sleep(200);
      } catch (e) {
        console.log(`  ❌ "${keyword}" - ${(e as Error).message}`);
      }
    }
  }

  await producer.disconnect();

  console.log(`\n${"=".repeat(60)}`);
  console.log("📊 Google 검색 수집 완료");
  console.log("=".repeat(60));
  console.log(`  총 Kafka 메시지: ${totalMessages}건`);
  console.log(`  저장 토픽: ${GOOGLE_SEARCH_TOPIC}`);
  console.log("=".repeat(60));
}

main().catch((e) => {
  console.error("치명적 오류:", (e as Error).message);
  process.exit(1);
});
