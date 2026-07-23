# 커뮤니티 수집 및 AI 요약 현황

## 처리 구조

크롤러는 사이트별 인기글 목록과 본문을 수집해 PostgreSQL `boards` 테이블에 저장한다. 새 글은
`analysis_status=pending` 상태가 되며, `board-service`의 공통 분석 워커가 사이트 구분 없이
제목·본문을 AI로 요약하고 태그와 참여도 점수를 기록한다. 따라서 새 사이트를 추가할 때
백엔드에 사이트별 분기 코드를 추가할 필요는 없다.

수집 흐름은 다음과 같다.

1. 인기글 목록에서 원문 ID, 제목, 작성 시각, 댓글·추천·조회 지표를 읽는다.
2. `site:category:no`를 고유 원본 ID로 사용해 중복을 방지한다.
3. 신규 글은 본문과 미디어를 저장하고 AI 분석 대기열에 넣는다.
4. 기존 글은 본문을 다시 받지 않고 원문 지표와 인기점수만 갱신한다.

## 사이트별 범위

| site 값 | 수집 화면 | 분류 방식 | 원문 지표 |
|---|---|---|---|
| `dcinside` | 실시간 베스트 | 원본 갤러리 | 댓글, 추천, 조회 |
| `ppomppu` | HOT 게시글 | 원본 게시판 | 댓글, 추천, 조회 |
| `ygosu` | 실시간 인기글 | 원본 게시판 | 댓글, 추천, 조회 |
| `theqoo` | HOT | `hot` | 댓글, 조회 |
| `fmkorea` | 포텐터짐 | `best` | 댓글, 추천 |
| `arca` | 실시간 베스트 | 원본 채널 | 댓글, 추천, 조회 |
| `inven` | 오늘의 핫벤 | 원본 게시판 종류와 번호 | 댓글, 추천, 조회 |

더쿠는 일반 `/hot` 요청에서 접근 거부가 발생할 수 있어 실제 HOT 화면과 동일한
`/hot?filter_mode=normal` 경로와 브라우저 요청 헤더를 사용한다.

운영 서버의 데이터센터 IP를 커뮤니티가 차단하는 경우 `CRAWLER_HTTP_PROXY`에 사설망
HTTP 프록시 URL을 지정한다. 이 값은 새 인기글 수집기에만 적용하며, 설정하지 않은 로컬
환경에서는 직접 요청한다.

## 운영 점검

```sql
SELECT site,
       count(*) AS total,
       count(*) FILTER (WHERE analysis_status = 'done') AS done,
       count(*) FILTER (WHERE analysis_status = 'pending') AS pending,
       count(*) FILTER (WHERE analysis_status = 'failed') AS failed,
       max(created_at) AS latest
FROM boards
GROUP BY site
ORDER BY site;
```

신규 사이트 배포 직후에는 각 사이트 행이 생성되는지, `pending`이 잠시 증가한 뒤 `done`으로
전환되는지 확인한다. 목록 HTML 구조가 변경되면 해당 사이트가 0건을 반환하므로 크롤러 로그의
목록 오류와 사이트별 최근 적재 시각을 함께 감시한다.
