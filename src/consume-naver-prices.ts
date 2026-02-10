import { kafka, NAVER_INGREDIENT_PRICES_TOPIC, KAFKA_GROUP_ID } from "./lib.js";

interface ProductInfo {
  title: string;
  lprice: number;
  hprice: number;
  mallName: string;
  brand: string;
  link: string;
}

interface IngredientStats {
  unit: string;
  total: number;
  keywords: Record<string, number>;
  prices: number[];
  products: ProductInfo[];
}

async function main() {
  const admin = kafka.admin();
  await admin.connect();
  const existingTopics = await admin.listTopics();
  await admin.disconnect();

  if (!existingTopics.includes(NAVER_INGREDIENT_PRICES_TOPIC)) {
    console.log("⚠️  가격 토픽이 없습니다. 먼저 collect-naver-prices를 실행해주세요.");
    return;
  }

  const consumer = kafka.consumer({ groupId: `${KAFKA_GROUP_ID}-naver-prices-${Date.now()}` });
  await consumer.connect();
  await consumer.subscribe({
    topics: [NAVER_INGREDIENT_PRICES_TOPIC],
    fromBeginning: true,
  });

  const stats: Record<string, IngredientStats> = {};

  let resolve: () => void;
  const done = new Promise<void>((r) => { resolve = r; });
  const timeout = setTimeout(() => resolve(), 20000);

  await consumer.run({
    eachMessage: async ({ message }) => {
      try {
        const val = JSON.parse(message.value?.toString() ?? "{}");
        const ingredient = val.ingredient || "unknown";
        const keyword = val.keyword || "unknown";

        if (!stats[ingredient]) {
          stats[ingredient] = { unit: val.unit || "", total: 0, keywords: {}, prices: [], products: [] };
        }

        stats[ingredient].total++;
        stats[ingredient].keywords[keyword] = (stats[ingredient].keywords[keyword] || 0) + 1;

        if (val.product) {
          const lp = val.product.lprice || 0;
          if (lp > 0) stats[ingredient].prices.push(lp);
          stats[ingredient].products.push({
            title: val.product.title,
            lprice: lp,
            hprice: val.product.hprice || 0,
            mallName: val.product.mallName,
            brand: val.product.brand,
            link: val.product.link,
          });
        }
      } catch {
        // skip
      }
    },
  });

  await done;
  clearTimeout(timeout);
  await consumer.disconnect();

  // ── 결과 출력 ──
  console.log(`\n${"=".repeat(70)}`);
  console.log("🍪 두바이 쫀득 쿠키 재료 + 완제품 실제 판매 가격표");
  console.log(`   수집 시각: ${new Date().toLocaleString("ko-KR")}`);
  console.log("=".repeat(70));

  const sortedIngredients = Object.entries(stats);
  let grandTotal = 0;

  // ── 가격 요약 테이블 ──
  console.log("\n┌─────────────────────────────┬──────────┬──────────┬──────────┬──────────┐");
  console.log("│ 재료                        │ 최저가   │ 평균가   │ 최고가   │ 수집건수 │");
  console.log("├─────────────────────────────┼──────────┼──────────┼──────────┼──────────┤");

  for (const [ingredient, data] of sortedIngredients) {
    grandTotal += data.total;
    if (data.prices.length === 0) continue;

    const min = Math.min(...data.prices);
    const max = Math.max(...data.prices);
    const avg = Math.round(data.prices.reduce((a, b) => a + b, 0) / data.prices.length);

    const name = `${ingredient}`.padEnd(20);
    const minStr = `${min.toLocaleString()}원`.padStart(8);
    const avgStr = `${avg.toLocaleString()}원`.padStart(8);
    const maxStr = `${max.toLocaleString()}원`.padStart(8);
    const cntStr = `${data.total}건`.padStart(8);

    console.log(`│ ${name}        │ ${minStr} │ ${avgStr} │ ${maxStr} │ ${cntStr} │`);
  }

  console.log("└─────────────────────────────┴──────────┴──────────┴──────────┴──────────┘");

  // ── 재료별 상세 ──
  for (const [ingredient, data] of sortedIngredients) {
    console.log(`\n── 🧂 ${ingredient} (${data.unit}) | ${data.total}건 ──`);

    if (data.prices.length > 0) {
      const min = Math.min(...data.prices);
      const max = Math.max(...data.prices);
      const avg = Math.round(data.prices.reduce((a, b) => a + b, 0) / data.prices.length);
      const median = data.prices.sort((a, b) => a - b)[Math.floor(data.prices.length / 2)];
      console.log(`  💰 최저 ${min.toLocaleString()}원 | 평균 ${avg.toLocaleString()}원 | 중간값 ${median.toLocaleString()}원 | 최고 ${max.toLocaleString()}원`);
    }

    // 최저가 TOP 5
    const top5 = data.products
      .filter((p) => p.lprice > 0)
      .sort((a, b) => a.lprice - b.lprice)
      .filter((v, i, arr) => arr.findIndex((x) => x.title === v.title) === i)
      .slice(0, 5);

    if (top5.length > 0) {
      console.log("  🏷️  최저가 TOP 5:");
      top5.forEach((p, i) => {
        const mall = p.mallName || p.brand || "기타";
        console.log(
          `    ${i + 1}. ${p.lprice.toLocaleString()}원 - ${p.title.substring(0, 50)} (${mall})`,
        );
      });
    }
  }

  console.log(`\n${"=".repeat(70)}`);
  console.log(`📊 총 수집: ${grandTotal}건 | 재료: ${sortedIngredients.length}종`);
  console.log("=".repeat(70));
}

main().catch((e) => {
  console.error("오류:", (e as Error).message);
  process.exit(1);
});
