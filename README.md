# CrawlScheduler
사이트를 크롤링하는 스케쥴러

## 소개
CrawlScheduler는 다양한 커뮤니티 사이트에서 게시물을 크롤링하고, 이를 데이터베이스에 저장하는 스케줄러입니다. 이 프로젝트는 Ygosu, Ppomppu 사이트를 대상으로 하며, 향후 추가될 예정입니다.

## 기능
- **실시간 베스트 게시물 수집**: 실시간 베스트 게시물을 크롤링하여 데이터베이스에 저장합니다.
- **게시물 내용 수집**: 게시물의 내용을 이미지, 비디오, 텍스트 형식으로 수집합니다.

## 사용 방법
1. **환경 설정**: 필요한 라이브러리를 설치합니다.
   ```bash
   poetry install
   ```

2. **데이터베이스 설정**: PostgreSQL 연결 정보를 `.env`에 설정합니다.

   ```env
   DATABASE_URL=postgresql+psycopg://<user>:<password>@localhost:5432/<db>
   DOCKER_DATABASE_URL=postgresql+psycopg://<user>:<password>@kingwangjjang-postgres:5432/<db>
   ```

3. **스크립트 실행**: 아래 명령어로 스크립트를 실행하여 크롤링을 시작합니다.
   ```bash
   dev.sh
   ```

   초기 데이터 적재처럼 한 번만 크롤링하고 종료하려면 아래처럼 실행합니다.
   ```bash
   poetry run python crawl_scheduler/main.py --once
   ```

## 파일 구조
crawl_scheduler/

│

├── community_website/

│ ├── ygosu.py # Ygosu 사이트 크롤러

│ ├── ppomppu.py # Ygosu 사이트 크롤러

│

├── db/

│ ├── postgres.py # PostgreSQL 연결 설정

│ ├── postgres_controller.py # PostgreSQL 저장 컨트롤러

│

├── constants.py # 상수 정의

│

├── utils/

│ ├── loghandler.py # 로깅 핸들러

│

└── main.py # 메인 실행 파일


## PostgreSQL 저장 방식
크롤러는 `boards` 테이블에 게시물을 저장합니다. 중복 저장 방지는 `source_id`로 처리하며, `source_id`는 기본적으로 `site:category:no` 형식을 사용합니다.

컨테이너 실행 시에는 `DOCKER_DATABASE_URL`이 `DATABASE_URL`로 주입됩니다. 로컬에서 직접 실행할 때는 `DATABASE_URL`을 설정하면 됩니다.


## 기여
기여를 원하시는 분은 이 저장소를 포크한 후, 변경 사항을 제안해 주세요. 

## 라이센스
이 프로젝트는 MIT 라이센스 하에 배포됩니다.
