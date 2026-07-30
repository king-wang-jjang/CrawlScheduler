# CrawlScheduler

커뮤니티 인기 게시물을 수집해 PostgreSQL에 저장하는 스케줄러입니다.

현재 디시인사이드, 뽐뿌, 와이고수, 더쿠, 에펨코리아, 아카라이브, 인벤의 인기글을
사이트별 주기로 수집합니다. 신규 글은 `analysis_status=pending`으로 저장되며
`board-service`의 공통 AI 분석 워커가 요약·태그·참여도 점수를 생성합니다.

## 실행

```bash
poetry install
poetry run python crawl_scheduler/main.py --once
poetry run python crawl_scheduler/main.py --run-on-start --interval-minutes 5
```

필수 환경 변수는 PostgreSQL 연결 문자열입니다.

```env
DATABASE_URL=postgresql+psycopg://<user>:<password>@localhost:5432/<db>
DOCKER_DATABASE_URL=postgresql+psycopg://<user>:<password>@kingwangjjang-postgres:5432/<db>
# 기본 주기와 사이트별 주기(분)
CRAWLER_INTERVAL_MINUTES=5
CRAWLER_ARCA_INTERVAL_MINUTES=10
CRAWLER_THEQOO_INTERVAL_MINUTES=15
CRAWLER_FMKOREA_INTERVAL_MINUTES=30
# 임시 수집 중지 목록
CRAWLER_DISABLED_SITES=fmkorea,theqoo,arca
# 데이터센터 IP가 차단될 때만 사용하는 선택값
CRAWLER_HTTP_PROXY=http://<private-proxy-host>:3128
```

환경변수가 없을 때의 권장 기본값은 와이고수·뽐뿌·디시인사이드·인벤 5분,
아카라이브 10분, 더쿠 15분, 에펨코리아 30분입니다. 사이트별 환경변수는
`CRAWLER_INTERVAL_MINUTES`보다 우선합니다. 명령행의 `--interval-minutes`는
테스트용으로 모든 사이트를 같은 값으로 강제하며 모든 환경변수보다 우선합니다.
주기는 1분 이상의 정수만 허용합니다.

`CRAWLER_DISABLED_SITES`에 쉼표로 구분한 사이트를 지정하면 반복 스케줄과
`--once` 실행에서 모두 제외됩니다. 현재 운영에서는 원문 접근이 차단된
에펨코리아·더쿠·아카라이브를 임시 중지하고 나머지 네 사이트만 수집합니다.

HTTP 429/430 응답을 받은 사이트는 `Retry-After` 동안 추가 요청하지 않고 해당
사이트의 현재 수집만 중단합니다. 차단된 본문은 빈 게시물로 저장하지 않으며 다른
사이트의 예약 작업은 계속 실행됩니다.

컨테이너에서 수집한 미디어는 `CRAWLER_MEDIA_HOST_ROOT`(기본값
`/mnt/kingwangjjang`)에 저장됩니다. 게시글 미디어는 사이트/게시판/연/월/일/글 번호로
분산해 단일 디렉터리의 엔트리 수가 과도하게 증가하지 않도록 합니다.

## 저장 데이터 품질 감사

운영 DB의 게시글이 AI 분석에 충분한지 사이트별 JSON으로 확인할 수 있습니다. 이
명령은 `boards`를 읽기만 하며 스키마나 데이터를 변경하지 않습니다.

```bash
poetry run python scripts/audit_content_quality.py --pretty
```

`sufficient_body`는 본문 판정 기준을 통과한 수, `needs_refresh`와
`title_only_or_insufficient`는 본문이 부족한 수입니다. `image_recoverable`은
`sufficient_body` 중 `ROOT` 아래에 실제로 존재하고 읽을 수 있는 로컬 이미지로
복구 가능한 글의 부분집합입니다. 기본 출력은 CI에서 처리하기 쉬운 한 줄 JSON이며
`--pretty`를 지정하면 들여씁니다.

본문 기준은 `AI_ANALYSIS_MIN_BODY_CHARS`(기본 20)와
`AI_ANALYSIS_MIN_LANGUAGE_CHARS`(기본 4)로 board-service와 동일하게 맞춥니다.
기준 미달 글은 인기 목록에 다시 노출될 때 본문을 복구하되,
`CONTENT_REFRESH_COOLDOWN_SECONDS`(기본 900초) 동안 반복 요청을 쉬고 복구
과정에서는 영상을 다시 내려받지 않아 고아 미디어가 쌓이지 않게 합니다.

사이트별 수집 범위와 AI 요약 흐름, 운영 점검 SQL은
[docs/SITE_COVERAGE.md](docs/SITE_COVERAGE.md)를 참고하세요.
