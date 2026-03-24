# SQL 코드 레벨 분석 - 공부용 가이드

이 프로젝트에서 사용한 모든 SQL을 한 줄씩 분해해서 설명합니다.

---

# 레벨 1: 기본 조회

## 채널별 ROI 조회

```sql
SELECT channel, avg_roi, avg_conversion_rate
FROM lexus_ad.mart_channel
ORDER BY avg_roi DESC
```

| 줄 | 코드 | 뜻 |
|-----|------|-----|
| 1 | `SELECT channel, avg_roi, avg_conversion_rate` | 이 3개 컬럼을 보여줘 |
| 2 | `FROM lexus_ad.mart_channel` | mart_channel 테이블에서 |
| 3 | `ORDER BY avg_roi DESC` | avg_roi 높은 순으로 정렬 |

> **핵심**: SELECT(뭘 볼지) + FROM(어디서) + ORDER BY(어떤 순서로)

---

## 매출 상위 10개 조회

```sql
SELECT TV, Radio, Newspaper, Sales
FROM advertising.ad_sales
WHERE TV >= 100
ORDER BY Sales DESC
LIMIT 10
```

| 줄 | 코드 | 뜻 |
|-----|------|-----|
| 1 | `SELECT TV, Radio, Newspaper, Sales` | 이 4개 컬럼을 보여줘 |
| 2 | `FROM advertising.ad_sales` | ad_sales 테이블에서 |
| 3 | `WHERE TV >= 100` | TV 광고비가 100 이상인 것만 |
| 4 | `ORDER BY Sales DESC` | 매출 높은 순 |
| 5 | `LIMIT 10` | 10개만 |

> **핵심**: WHERE = 조건 필터. "이것만 보여줘"

---

# 레벨 2: CASE WHEN (조건 분기)

## TV 예산 구간별 매출

```sql
SELECT
  CASE
    WHEN TV < 100 THEN '저예산'
    WHEN TV < 200 THEN '중예산'
    ELSE '고예산'
  END AS tv_budget_group,
  COUNT(*) AS cnt,
  ROUND(AVG(Sales), 1) AS avg_sales
FROM advertising.ad_sales
GROUP BY tv_budget_group
ORDER BY avg_sales DESC
```

| 줄 | 코드 | 뜻 |
|-----|------|-----|
| 2-6 | `CASE WHEN ... END AS tv_budget_group` | TV 값에 따라 이름표 붙이기 |
| 3 | `WHEN TV < 100 THEN '저예산'` | TV가 100 미만이면 → "저예산" |
| 4 | `WHEN TV < 200 THEN '중예산'` | TV가 200 미만이면 → "중예산" |
| 5 | `ELSE '고예산'` | 나머지 → "고예산" |
| 7 | `COUNT(*) AS cnt` | 각 그룹에 몇 개 있는지 세기 |
| 8 | `ROUND(AVG(Sales), 1) AS avg_sales` | 평균 매출, 소수점 1자리 |
| 10 | `GROUP BY tv_budget_group` | 위에서 만든 그룹별로 묶기 |

> **핵심**: CASE WHEN = "만약 ~이면 ~라고 불러라". 숫자를 의미 있는 그룹으로 나눌 때 씀

---

## TV + Radio 시너지 분석

```sql
SELECT
  CASE WHEN TV > 100 THEN 'TV높음' ELSE 'TV낮음' END AS tv_level,
  CASE WHEN Radio > 20 THEN 'Radio높음' ELSE 'Radio낮음' END AS radio_level,
  ROUND(AVG(Sales), 1) AS avg_sales,
  COUNT(*) AS cnt
FROM advertising.ad_sales
GROUP BY tv_level, radio_level
ORDER BY avg_sales DESC
```

| 줄 | 코드 | 뜻 |
|-----|------|-----|
| 2 | `CASE WHEN TV > 100 THEN 'TV높음' ELSE 'TV낮음' END` | TV가 100 넘으면 "높음", 아니면 "낮음" |
| 3 | `CASE WHEN Radio > 20 THEN 'Radio높음' ELSE 'Radio낮음' END` | Radio도 마찬가지 |
| 7 | `GROUP BY tv_level, radio_level` | 2개 기준으로 묶기 → 2×2 = 4개 조합 |

> **핵심**: CASE WHEN을 여러 개 쓰면 다차원 분석 가능. "TV높음+Radio높음" 같은 조합별 성과를 볼 수 있음

---

# 레벨 3: 집계함수 + 데이터 가공

## 데이터 마트 생성 (mart_monthly)

```sql
CREATE OR REPLACE TABLE lexus_ad.mart_monthly AS
SELECT
  FORMAT_DATE("%Y-%m", Date) AS month,
  SUM(Impressions) AS total_impressions,
  SUM(Clicks) AS total_clicks,
  ROUND(AVG(Conversion_Rate), 4) AS avg_conversion_rate,
  ROUND(AVG(ROI), 2) AS avg_roi,
  ROUND(AVG(CAST(REGEXP_REPLACE(Acquisition_Cost, r'[^0-9.]', '') AS FLOAT64)), 2) AS avg_acquisition_cost,
  COUNT(*) AS campaign_count
FROM lexus_ad.campaign_adjusted
GROUP BY month
ORDER BY month
```

| 줄 | 코드 | 뜻 |
|-----|------|-----|
| 1 | `CREATE OR REPLACE TABLE ... AS` | 새 테이블 만들어라. 이미 있으면 덮어써라 |
| 3 | `FORMAT_DATE("%Y-%m", Date)` | 날짜를 "2021-03" 형태로 바꿔라 |
| 4 | `SUM(Impressions)` | 노출수 전부 더하기 |
| 5 | `SUM(Clicks)` | 클릭수 전부 더하기 |
| 6 | `ROUND(AVG(...), 4)` | 평균 내고 소수점 4자리 반올림 |
| 8 | `REGEXP_REPLACE(Acquisition_Cost, r'[^0-9.]', '')` | "$250.50"에서 $ 제거 → "250.50" |
| 8 | `CAST(... AS FLOAT64)` | 문자열 "250.50"을 숫자 250.50으로 변환 |
| 8 | `AVG(...)` | 평균 |
| 8 | `ROUND(..., 2)` | 소수점 2자리 반올림 |
| 11 | `GROUP BY month` | 월별로 묶어서 위의 SUM, AVG 계산 |

> **핵심**: 한 줄에 함수가 여러 개 겹쳐있으면 **안쪽부터** 읽어라
> `REGEXP_REPLACE` → `CAST` → `AVG` → `ROUND` 순서로 실행됨

---

## 채널별 ROI (0 나누기 방지)

```sql
SELECT
  ROUND(AVG(Sales / NULLIF(TV, 0)), 2) AS tv_roi,
  ROUND(AVG(Sales / NULLIF(Radio, 0)), 2) AS radio_roi,
  ROUND(AVG(Sales / NULLIF(Newspaper, 0)), 2) AS newspaper_roi
FROM advertising.ad_sales
WHERE TV > 0 AND Radio > 0 AND Newspaper > 0
```

| 줄 | 코드 | 뜻 |
|-----|------|-----|
| 2 | `NULLIF(TV, 0)` | TV가 0이면 NULL로 바꿔라 (0으로 나누면 에러나니까) |
| 2 | `Sales / NULLIF(TV, 0)` | 매출 ÷ TV광고비 = TV 채널 ROI |
| 2 | `AVG(...)` | 전체 평균 |
| 6 | `WHERE TV > 0 AND Radio > 0` | 광고비가 0인 행은 제외 |

> **핵심**: `NULLIF(값, 0)` = 0으로 나누는 에러를 방지하는 안전장치

---

# 레벨 4: 파티션 & 클러스터링

## 월별 파티션 테이블

```sql
CREATE TABLE lexus_ad.campaign_partitioned
PARTITION BY DATE_TRUNC(Date, MONTH)
AS SELECT * FROM lexus_ad.campaign_raw
```

| 줄 | 코드 | 뜻 |
|-----|------|-----|
| 1 | `CREATE TABLE ...` | 새 테이블 만들어라 |
| 2 | `PARTITION BY DATE_TRUNC(Date, MONTH)` | Date를 월 단위로 잘라서 서랍 나눠라 |
| 3 | `AS SELECT * FROM campaign_raw` | 원본 데이터 전부 가져와서 넣어라 |

> `DATE_TRUNC(Date, MONTH)` = 2021-03-15 → 2021-03-01 (월 단위로 자름)
> 같은 월의 데이터가 같은 서랍에 들어감

---

## 일별 파티션 테이블

```sql
CREATE TABLE lexus_ad.campaign_partitioned_daily
PARTITION BY DATE(Date)
AS SELECT * FROM lexus_ad.campaign_raw
```

| 줄 | 코드 | 뜻 |
|-----|------|-----|
| 2 | `PARTITION BY DATE(Date)` | 날짜별로 서랍 나눠라 (365개 서랍) |

> 월별보다 더 세밀하게 나눔. 특정 날짜 조회 시 99.7% 비용 절감

---

## 파티션 + 클러스터링

```sql
CREATE TABLE lexus_ad.campaign_clustered
PARTITION BY DATE(Date)
CLUSTER BY Channel_Used, Campaign_Type, Location
AS SELECT * FROM lexus_ad.campaign_adjusted
```

| 줄 | 코드 | 뜻 |
|-----|------|-----|
| 2 | `PARTITION BY DATE(Date)` | 날짜별로 서랍 나누고 |
| 3 | `CLUSTER BY Channel_Used, Campaign_Type, Location` | 서랍 안에서 이 3개 기준으로 정렬 |

> 파티션 = 서랍 나누기
> 클러스터링 = 서랍 안에서 비슷한 것끼리 모아두기
> WHERE Channel_Used = 'YouTube' 하면 YouTube 묶음만 읽어서 더 빠름

---

# 레벨 5: CTE + 윈도우 함수

## RFM 분석 (가장 복잡한 쿼리)

이 쿼리는 3단계로 나뉨:

### 1단계: RFM 원시값 계산 (rfm_raw)

```sql
WITH rfm_raw AS (
  SELECT
    Company,
    Channel_Used AS channel,
    DATE_DIFF(DATE '2021-12-31', MAX(DATE(Date)), DAY) AS recency,
    COUNT(*) AS frequency,
    ROUND(SUM(ROI), 2) AS monetary
  FROM lexus_ad.campaign_adjusted
  GROUP BY Company, Channel_Used
)
```

| 줄 | 코드 | 뜻 |
|-----|------|-----|
| 1 | `WITH rfm_raw AS (...)` | "rfm_raw"라는 임시 테이블을 먼저 만들어라 |
| 5 | `DATE_DIFF(DATE '2021-12-31', MAX(DATE(Date)), DAY)` | 12월 31일에서 마지막 캠페인 날짜까지 며칠 차이? = 최근성(R) |
| 6 | `COUNT(*)` | 캠페인 총 횟수 = 빈도(F) |
| 7 | `ROUND(SUM(ROI), 2)` | ROI 총합 = 수익(M) |
| 9 | `GROUP BY Company, Channel_Used` | 회사 × 채널별로 묶기 → 30개 조합 |

> `WITH ... AS` = CTE(Common Table Expression). 복잡한 쿼리를 단계별로 나눠서 읽기 쉽게 만드는 방법.
> 실제 테이블이 아니라 쿼리 안에서만 쓰는 임시 결과물.

### 2단계: 점수 매기기 (rfm_scored)

```sql
rfm_scored AS (
  SELECT *,
    NTILE(5) OVER (ORDER BY recency DESC) AS r_score,
    NTILE(5) OVER (ORDER BY frequency ASC) AS f_score,
    NTILE(5) OVER (ORDER BY monetary ASC) AS m_score
  FROM rfm_raw
)
```

| 줄 | 코드 | 뜻 |
|-----|------|-----|
| 2 | `SELECT *` | rfm_raw의 모든 컬럼 가져오고 |
| 3 | `NTILE(5) OVER (ORDER BY recency DESC)` | recency를 정렬해서 5등분 → 1~5점 |
| 4 | `NTILE(5) OVER (ORDER BY frequency ASC)` | frequency를 정렬해서 5등분 → 1~5점 |
| 5 | `NTILE(5) OVER (ORDER BY monetary ASC)` | monetary를 정렬해서 5등분 → 1~5점 |

> `NTILE(5)` = 데이터를 5등분해서 각각 1~5 번호를 매기는 윈도우 함수
> `OVER (ORDER BY ...)` = 이 순서로 정렬한 다음에 나눠라
> recency는 DESC(낮을수록 좋으니까), frequency와 monetary는 ASC(높을수록 좋으니까)

### 3단계: 등급 분류

```sql
SELECT *,
  ROUND((r_score + f_score + m_score) / 3.0, 1) AS rfm_avg,
  CASE
    WHEN (r_score + f_score + m_score) >= 12 THEN 'VIP'
    WHEN (r_score + f_score + m_score) >= 9 THEN 'Loyal'
    WHEN (r_score + f_score + m_score) >= 6 THEN 'Normal'
    ELSE 'At Risk'
  END AS segment
FROM rfm_scored
```

| 줄 | 코드 | 뜻 |
|-----|------|-----|
| 2 | `(r_score + f_score + m_score) / 3.0` | 3개 점수 평균 |
| 4 | `WHEN ... >= 12 THEN 'VIP'` | 합계 12 이상이면 VIP |
| 5 | `WHEN ... >= 9 THEN 'Loyal'` | 9 이상이면 Loyal |
| 6 | `WHEN ... >= 6 THEN 'Normal'` | 6 이상이면 Normal |
| 7 | `ELSE 'At Risk'` | 나머지는 At Risk |

> 1단계에서 원시값 만들고 → 2단계에서 점수 매기고 → 3단계에서 등급 분류
> CTE 덕분에 한 번에 다 쓰지 않고 단계별로 나눠서 읽기 쉬움

---

## 월별 성장률 (LAG)

```sql
SELECT
  month,
  avg_roi,
  ROUND(avg_roi - LAG(avg_roi) OVER (ORDER BY month), 2) AS roi_change,
  ROUND((avg_roi - LAG(avg_roi) OVER (ORDER BY month)) / LAG(avg_roi) OVER (ORDER BY month) * 100, 1) AS roi_growth_pct
FROM lexus_ad.mart_monthly
ORDER BY month
```

| 줄 | 코드 | 뜻 |
|-----|------|-----|
| 4 | `LAG(avg_roi) OVER (ORDER BY month)` | 이전 달의 avg_roi를 가져와라 |
| 4 | `avg_roi - LAG(...)` | 이번 달 - 지난 달 = 변화량 |
| 5 | `(...) / LAG(...) * 100` | 변화량 ÷ 지난 달 × 100 = 성장률(%) |

> `LAG(컬럼)` = "이전 행의 값"을 가져오는 윈도우 함수
> 1월의 LAG는 없으니까 NULL이 됨 (비교할 이전 달이 없으니까)

---

## 누적 합계 (SUM OVER)

```sql
SELECT
  month,
  total_clicks,
  SUM(total_clicks) OVER (ORDER BY month) AS cumulative_clicks
FROM lexus_ad.mart_monthly
ORDER BY month
```

| 줄 | 코드 | 뜻 |
|-----|------|-----|
| 4 | `SUM(total_clicks) OVER (ORDER BY month)` | 월 순서대로 클릭수를 누적해서 더해라 |

> 1월: 1월 값
> 2월: 1월 + 2월
> 3월: 1월 + 2월 + 3월
> ...이렇게 쌓임

---

## 이동 평균 (AVG OVER ROWS BETWEEN)

```sql
SELECT
  month,
  avg_roi,
  ROUND(AVG(avg_roi) OVER (ORDER BY month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS moving_avg_3m
FROM lexus_ad.mart_monthly
ORDER BY month
```

| 줄 | 코드 | 뜻 |
|-----|------|-----|
| 4 | `ROWS BETWEEN 2 PRECEDING AND CURRENT ROW` | 현재 행 포함 이전 2개 행 (= 3개월) |
| 4 | `AVG(...) OVER (...)` | 이 범위의 평균 |

> 3월의 이동평균 = (1월 + 2월 + 3월) / 3
> 6월의 이동평균 = (4월 + 5월 + 6월) / 3
> 단기 변동을 제거하고 추세를 볼 때 씀

---

## 월별 채널 순위 (RANK OVER PARTITION BY)

```sql
WITH channel_monthly AS (
  SELECT
    FORMAT_DATE("%Y-%m", Date) AS month,
    Channel_Used AS channel,
    ROUND(AVG(ROI), 2) AS avg_roi
  FROM lexus_ad.campaign_adjusted
  GROUP BY month, channel
)
SELECT
  month,
  channel,
  avg_roi,
  RANK() OVER (PARTITION BY month ORDER BY avg_roi DESC) AS rank
FROM channel_monthly
ORDER BY month, rank
```

| 줄 | 코드 | 뜻 |
|-----|------|-----|
| 1-8 | `WITH channel_monthly AS (...)` | 먼저 월별×채널별 평균 ROI 계산 |
| 13 | `PARTITION BY month` | 월별로 나눠서 |
| 13 | `ORDER BY avg_roi DESC` | ROI 높은 순으로 |
| 13 | `RANK()` | 순위 매기기 (1등, 2등, 3등...) |

> `PARTITION BY` = GROUP BY와 비슷하지만, 행을 줄이지 않고 그룹 안에서 계산만 함
> 매달 6개 채널의 순위가 나옴 → 12개월 × 6채널 = 72행

---

# 데이터 보정 쿼리

## 원본 데이터에 가중치 적용

```sql
CREATE OR REPLACE TABLE lexus_ad.campaign_adjusted AS
SELECT
  ...(기존 컬럼들),

  -- 채널별 ROI 보정
  ROUND(ROI * CASE Channel_Used
    WHEN 'YouTube' THEN 1.6
    WHEN 'Instagram' THEN 1.4
    WHEN 'Facebook' THEN 1.1
    WHEN 'Google Ads' THEN 1.0
    WHEN 'Website' THEN 0.7
    WHEN 'Email' THEN 0.4
  END * CASE Location
    WHEN 'Los Angeles' THEN 1.3
    WHEN 'Miami' THEN 1.2
    WHEN 'New York' THEN 1.1
    WHEN 'Chicago' THEN 0.9
    WHEN 'Houston' THEN 0.8
  END * CASE
    WHEN EXTRACT(MONTH FROM Date) >= 9 THEN 1.2
    WHEN EXTRACT(MONTH FROM Date) >= 6 THEN 1.1
    ELSE 1.0
  END, 2) AS ROI

FROM lexus_ad.campaign_raw
```

| 부분 | 코드 | 뜻 |
|------|------|-----|
| 채널 가중치 | `CASE Channel_Used WHEN 'YouTube' THEN 1.6` | YouTube면 ROI에 1.6배 |
| 지역 가중치 | `CASE Location WHEN 'Los Angeles' THEN 1.3` | LA면 1.3배 |
| 시즌 가중치 | `WHEN EXTRACT(MONTH FROM Date) >= 9 THEN 1.2` | 9월 이후면 1.2배 |
| 최종 | `ROI * 채널 * 지역 * 시즌` | 3개 가중치를 곱해서 최종 ROI |

> `EXTRACT(MONTH FROM Date)` = 날짜에서 월만 뽑기
> 원본 ROI에 채널별 × 지역별 × 시즌별 가중치를 곱해서 렉서스 시나리오에 맞게 보정

---

# 공부 순서 추천

```
1주차: SELECT, WHERE, ORDER BY, LIMIT (레벨 1)
       → BigQuery 콘솔에서 직접 쳐보기

2주차: CASE WHEN, GROUP BY (레벨 2)
       → 데이터를 그룹으로 나눠보기

3주차: SUM, AVG, COUNT, ROUND, CAST (레벨 3)
       → 데이터 마트 직접 만들어보기

4주차: PARTITION BY, CLUSTER BY (레벨 4)
       → 비용 최적화 이해하기

5주차: WITH(CTE), NTILE, LAG, RANK, OVER (레벨 5)
       → 윈도우 함수로 고급 분석 해보기
```

면접에서 레벨 3까지 설명 가능하면 기본, 레벨 5까지 하면 강력한 어필!
