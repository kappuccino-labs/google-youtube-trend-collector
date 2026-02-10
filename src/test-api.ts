import {
  GOOGLE_API_KEY,
  GOOGLE_CSE_ID,
  YOUTUBE_API_KEY,
  KAFKA_BOOTSTRAP_SERVERS,
  kafka,
  searchGoogle,
  searchYouTube,
  getYouTubeVideoStats,
} from "./lib.js";

let passed = 0;
let failed = 0;

function check(name: string, condition: boolean) {
  if (condition) {
    console.log(`  ✅ ${name}`);
    passed++;
  } else {
    console.log(`  ❌ ${name}`);
    failed++;
  }
}

async function main() {
  console.log("🔧 환경변수 확인");
  check("KAFKA_BOOTSTRAP_SERVERS 설정됨", !!KAFKA_BOOTSTRAP_SERVERS);
  check("GOOGLE_API_KEY 설정됨", !!GOOGLE_API_KEY);
  check("GOOGLE_CSE_ID 설정됨", !!GOOGLE_CSE_ID);
  check("YOUTUBE_API_KEY 설정됨", !!YOUTUBE_API_KEY);

  // Kafka 연결 테스트
  console.log("\n[1/4] Kafka 연결 테스트");
  try {
    const admin = kafka.admin();
    await admin.connect();
    const topics = await admin.listTopics();
    check("Kafka 클러스터 연결", true);
    console.log(`    토픽 수: ${topics.length}`);
    await admin.disconnect();
  } catch (e) {
    check(`Kafka 연결 실패: ${(e as Error).message}`, false);
  }

  // Google Custom Search 테스트
  console.log("\n[2/4] Google Custom Search API 테스트");
  if (GOOGLE_API_KEY && GOOGLE_CSE_ID) {
    try {
      const result = await searchGoogle("두바이 쿠키", 1, 3);
      check("API 응답 성공", !!result.searchInformation);
      check("검색 결과 존재", (result.items?.length ?? 0) > 0);
      console.log(`    총 결과: ${parseInt(result.searchInformation.totalResults).toLocaleString()}건`);
      console.log(`    수집: ${result.items?.length ?? 0}건`);
      if (result.items?.[0]) {
        console.log(`    첫 결과: ${result.items[0].title.substring(0, 60)}`);
      }
    } catch (e) {
      check(`Google API 오류: ${(e as Error).message}`, false);
    }
  } else {
    console.log("  ⏭️  GOOGLE_API_KEY 또는 GOOGLE_CSE_ID 미설정 - 건너뜀");
  }

  // YouTube Search 테스트
  console.log("\n[3/4] YouTube Search API 테스트");
  if (YOUTUBE_API_KEY) {
    try {
      const result = await searchYouTube("두바이 쿠키", 5, "date");
      check("API 응답 성공", !!result.pageInfo);
      check("검색 결과 존재", result.items.length > 0);
      console.log(`    총 결과: ${result.pageInfo.totalResults.toLocaleString()}건`);
      console.log(`    수집: ${result.items.length}건`);
      if (result.items[0]) {
        console.log(`    첫 영상: ${result.items[0].snippet.title.substring(0, 60)}`);
      }

      // YouTube Video Stats 테스트
      console.log("\n[4/4] YouTube Video Stats API 테스트");
      const videoIds = result.items
        .map((i) => i.id.videoId)
        .filter((id): id is string => !!id)
        .slice(0, 3);

      if (videoIds.length > 0) {
        const stats = await getYouTubeVideoStats(videoIds);
        check("통계 API 응답 성공", stats.items.length > 0);
        for (const v of stats.items) {
          console.log(
            `    📊 ${v.id}: 조회 ${parseInt(v.statistics.viewCount).toLocaleString()} / 좋아요 ${parseInt(v.statistics.likeCount).toLocaleString()}`,
          );
        }
      }
    } catch (e) {
      check(`YouTube API 오류: ${(e as Error).message}`, false);
    }
  } else {
    console.log("  ⏭️  YOUTUBE_API_KEY 미설정 - 건너뜀");
  }

  // 결과 요약
  console.log(`\n${"=".repeat(50)}`);
  console.log(`검증 결과: ${passed}/${passed + failed} 통과${failed > 0 ? ` (${failed}건 실패)` : " ✅ 모두 통과!"}`);
  console.log("=".repeat(50));

  if (failed > 0) process.exit(1);
}

main().catch((e) => {
  console.error("치명적 오류:", (e as Error).message);
  process.exit(1);
});
