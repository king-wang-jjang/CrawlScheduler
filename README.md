# CrawlScheduler

커뮤니티 인기 게시물을 수집해 PostgreSQL에 저장하는 스케줄러입니다.

현재 디시인사이드, 뽐뿌, 와이고수, 더쿠, 에펨코리아, 아카라이브, 인벤의 인기글을
5분 간격으로 수집합니다. 신규 글은 `analysis_status=pending`으로 저장되며
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
# 데이터센터 IP가 차단될 때만 사용하는 선택값
CRAWLER_HTTP_PROXY=http://<private-proxy-host>:3128
```

컨테이너에서 수집한 미디어는 `CRAWLER_MEDIA_HOST_ROOT`(기본값
`/mnt/kingwangjjang`)에 저장됩니다. 게시글 미디어는 사이트/게시판/연/월/일/글 번호로
분산해 단일 디렉터리의 엔트리 수가 과도하게 증가하지 않도록 합니다.

사이트별 수집 범위와 AI 요약 흐름, 운영 점검 SQL은
[docs/SITE_COVERAGE.md](docs/SITE_COVERAGE.md)를 참고하세요.
