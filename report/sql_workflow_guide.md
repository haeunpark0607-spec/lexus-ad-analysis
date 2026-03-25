# SQL 작업 순서 가이드 - 실제 프로젝트 흐름대로

이 프로젝트에서 실제로 한 순서대로 SQL을 정리합니다.

---

## 1단계: 데이터 업로드

Python으로 CSV 파일을 BigQuery에 업로드했음. SQL은 아님.

```python
from google.cloud import bigquery
client = bigquery.Client()
client.create_dataset('lexus_ad', exists_ok=True)
job = client.load_table_from_dataframe(df, 'lexus_ad.campaign_raw')
```

결과: `campaign_raw` 테이블 생성 (20만 행)

---

## 2단계: 데이터 보정 (가중치 적용)

원본 데이터가 너무 균일해서 채널별 차이가 없었음.
렉서스(프리미엄 브랜드) 시나리오에 맞게 ROI, 클릭수, 전환율에 가중치를 곱함.

```sql
CREATE OR REPLACE TABLE lexus_ad.campaign_adjusted AS
SELECT
  Campaign_ID, Company, Campaign_Type, Target_Audience,
  Duration, Channel_Used, Location, Language,
  Customer_Segment, Date, Engagement_Score, Acquisition_Cost,

  -- 클릭수 보정: 채널별로 다른 배수를 곱함
  CAST(Clicks * CASE Channel_Used
    WHEN 'YouTube' THEN 1.4      -- YouTube는 1.4배
    WHEN 'Instagram' THEN 1.3
    WHEN 'Facebook' THEN 1.1
    WHEN 'Google Ads' THEN 1.0   -- 기준점
    WHEN 'Website' THEN 0.8
    WHEN 'Email' THEN 0.6        -- Email은 0.6배로 줄임
  END AS INT64) AS Clicks,

  -- 노출수 보정: 같은 방식
  CAST(Impressions * CASE Channel_Used
    WHEN 'YouTube' THEN 1.5
    WHEN 'Instagram' THEN 1.3
    WHEN 'Facebook' THEN 1.1
    WHEN 'Google Ads' THEN 1.0
    WHEN 'Website' THEN 0.9
    WHEN 'Email' THEN 0.7
  END AS INT64) AS Impressions,

  -- 전환율 보정: 채널 가중치 × 캠페인유형 가중치
  ROUND(Conversion_Rate * CASE Channel_Used
    WHEN 'YouTube' THEN 1.5
    WHEN 'Instagram' THEN 1.4
    WHEN 'Facebook' THEN 1.1
    WHEN 'Google Ads' THEN 1.0
    WHEN 'Website' THEN 0.8
    WHEN 'Email' THEN 0.5
  END * CASE Campaign_Type
    WHEN 'Influencer' THEN 1.3   -- 인플루언서가 전환율 높게
    WHEN 'Social Media' THEN 1.2
    WHEN 'Display' THEN 1.0
    WHEN 'Search' THEN 0.9
    WHEN 'Email' THEN 0.7        -- 이메일 캠페인은 전환율 낮게
  END, 4) AS Conversion_Rate,

  -- ROI 보정: 채널 × 지역 × 시즌 3개 가중치를 곱함
  ROUND(ROI * CASE Channel_Used
    WHEN 'YouTube' THEN 1.6      -- YouTube가 ROI 가장 높게
    WHEN 'Instagram' THEN 1.4
    WHEN 'Facebook' THEN 1.1
    WHEN 'Google Ads' THEN 1.0
    WHEN 'Website' THEN 0.7
    WHEN 'Email' THEN 0.4        -- Email이 ROI 가장 낮게
  END * CASE Location
    WHEN 'Los Angeles' THEN 1.3  -- LA가 가장 높게 (고소득 지역)
    WHEN 'Miami' THEN 1.2
    WHEN 'New York' THEN 1.1
    WHEN 'Chicago' THEN 0.9
    WHEN 'Houston' THEN 0.8      -- Houston이 가장 낮게
  END * CASE
    WHEN EXTRACT(MONTH FROM Date) >= 9 THEN 1.2   -- 9월 이후 하반기 높게
    WHEN EXTRACT(MONTH FROM Date) >= 6 THEN 1.1   -- 6월 이후 약간 높게
    ELSE 1.0                                        -- 상반기는 기본
  END, 2) AS ROI

FROM lexus_ad.campaign_raw
```

### 왜 이렇게 했냐면
- `CASE Channel_Used WHEN 'YouTube' THEN 1.6` → YouTube면 ROI에 1.6배
- `CASE Location WHEN 'Los Angeles' THEN 1.3` → LA면 1.3배 추가
- `EXTRACT(MONTH FROM Date) >= 9 THEN 1.2` → 날짜에서 월만 뽑아서 9월 이후면 1.2배
- 최종: 원본 ROI × 채널배수 × 지역배수 × 시즌배수 = 보정된 ROI

---

## 3단계: 파티션 테이블 생성

데이터를 날짜 기준으로 나눠 저장. 조회할 때 필요한 부분만 읽어서 비용 절감.

### 월별 파티션

```sql
CREATE TABLE lexus_ad.campaign_partitioned
PARTITION BY DATE_TRUNC(Date, MONTH)
AS SELECT * FROM lexus_ad.campaign_raw
```

- `PARTITION BY` → 나눠라
- `DATE_TRUNC(Date, MONTH)` → 날짜를 월 단위로 잘라서
- 결과: 1월 서랍, 2월 서랍, ... 12월 서랍 = 12개 파티션

### 일별 파티션

```sql
CREATE TABLE lexus_ad.campaign_partitioned_daily
PARTITION BY DATE(Date)
AS SELECT * FROM lexus_ad.campaign_raw
```

- 365개 서랍으로 더 세밀하게 나눔
- 특정 날짜 조회 시 99.7% 비용 절감

### 파티션 + 클러스터링

```sql
CREATE TABLE lexus_ad.campaign_clustered
PARTITION BY DATE(Date)
CLUSTER BY Channel_Used, Campaign_Type, Location
AS SELECT * FROM lexus_ad.campaign_adjusted
```

- `PARTITION BY` → 날짜별로 서랍 나누고
- `CLUSTER BY` → 서랍 안에서 채널, 캠페인유형, 지역 순으로 정렬
- WHERE Channel_Used = 'YouTube' 하면 YouTube 묶음만 읽음

---

## 4단계: 데이터 마트 생성

원본 20만 행을 목적별로 요약한 테이블. 미리 만들어두면 매번 20만 행 스캔 안 해도 됨.

### 마트 1: 월별 요약

```sql
CREATE OR REPLACE TABLE lexus_ad.mart_monthly AS
SELECT
  FORMAT_DATE("%Y-%m", Date) AS month,     -- 2021-03-15 → "2021-03"
  SUM(Impressions) AS total_impressions,    -- 노출수 합계
  SUM(Clicks) AS total_clicks,             -- 클릭수 합계
  ROUND(AVG(Conversion_Rate), 4) AS avg_conversion_rate,  -- 전환율 평균
  ROUND(AVG(ROI), 2) AS avg_roi,           -- ROI 평균
  COUNT(*) AS campaign_count               -- 캠페인 몇 건인지
FROM lexus_ad.campaign_adjusted
GROUP BY month      -- 월별로 묶어서 위 계산
ORDER BY month      -- 월 순서로 정렬
```

결과: 20만 행 → 12행 (월별 요약)

### 마트 2: 채널별 요약

```sql
CREATE OR REPLACE TABLE lexus_ad.mart_channel AS
SELECT
  Channel_Used AS channel,
  SUM(Impressions) AS total_impressions,
  SUM(Clicks) AS total_clicks,
  ROUND(AVG(Conversion_Rate), 4) AS avg_conversion_rate,
  ROUND(AVG(ROI), 2) AS avg_roi,
  COUNT(*) AS campaign_count
FROM lexus_ad.campaign_adjusted
GROUP BY channel       -- 채널별로 묶기
ORDER BY avg_roi DESC  -- ROI 높은 순
```

결과: 20만 행 → 6행 (채널별 요약)

### 마트 3: 캠페인 유형별 요약

```sql
CREATE OR REPLACE TABLE lexus_ad.mart_campaign_type AS
SELECT
  Campaign_Type AS campaign_type,
  SUM(Impressions) AS total_impressions,
  SUM(Clicks) AS total_clicks,
  ROUND(AVG(Conversion_Rate), 4) AS avg_conversion_rate,
  ROUND(AVG(ROI), 2) AS avg_roi,
  COUNT(*) AS campaign_count
FROM lexus_ad.campaign_adjusted
GROUP BY campaign_type
ORDER BY avg_roi DESC
```

결과: 20만 행 → 5행

### 마트 4: 지역별 요약

```sql
CREATE OR REPLACE TABLE lexus_ad.mart_location AS
SELECT
  Location AS location,
  SUM(Impressions) AS total_impressions,
  SUM(Clicks) AS total_clicks,
  ROUND(AVG(Conversion_Rate), 4) AS avg_conversion_rate,
  ROUND(AVG(ROI), 2) AS avg_roi,
  COUNT(*) AS campaign_count
FROM lexus_ad.campaign_adjusted
GROUP BY location
ORDER BY avg_roi DESC
```

결과: 20만 행 → 5행

### 마트 5: CTR + 참여도 + ROI (병목 분석용)

```sql
CREATE OR REPLACE TABLE lexus_ad.mart_ctr_engagement AS
SELECT
  Channel_Used AS channel,
  -- CTR = 클릭수 ÷ 노출수 × 100
  ROUND(SUM(Clicks) / NULLIF(SUM(Impressions), 0) * 100, 2) AS ctr_pct,
  ROUND(AVG(Engagement_Score), 1) AS avg_engagement,
  ROUND(AVG(Conversion_Rate), 4) AS avg_conversion_rate,
  ROUND(AVG(ROI), 2) AS avg_roi
FROM lexus_ad.campaign_adjusted
GROUP BY channel
ORDER BY avg_roi DESC
```

- `SUM(Clicks) / NULLIF(SUM(Impressions), 0) * 100` → 클릭률(CTR) 직접 계산
- `NULLIF(..., 0)` → 노출수가 0이면 NULL로 바꿔서 0 나누기 에러 방지

### 마트 6: 세그먼트 × 채널

```sql
CREATE OR REPLACE TABLE lexus_ad.mart_segment_channel AS
SELECT
  Customer_Segment AS segment,
  Channel_Used AS channel,
  COUNT(*) AS campaign_count,
  ROUND(AVG(ROI), 2) AS avg_roi,
  ROUND(AVG(Conversion_Rate), 4) AS avg_conversion_rate,
  ROUND(AVG(Engagement_Score), 1) AS avg_engagement
FROM lexus_ad.campaign_adjusted
GROUP BY segment, channel    -- 2개 기준으로 묶기 → 5세그먼트 × 6채널 = 30행
ORDER BY segment, avg_roi DESC
```

---

## 5단계: RFM 분석 (고급)

회사 × 채널 = 30개 조합에 점수를 매겨서 등급 분류.
이 쿼리는 3단계로 나뉨.

```sql
CREATE OR REPLACE TABLE lexus_ad.mart_rfm AS

-- 1단계: 각 조합의 R, F, M 원시값 계산
WITH rfm_raw AS (
  SELECT
    Company,
    Channel_Used AS channel,
    -- R(최근성): 12월 31일에서 마지막 캠페인까지 며칠?
    DATE_DIFF(DATE '2021-12-31', MAX(DATE(Date)), DAY) AS recency,
    -- F(빈도): 캠페인 총 몇 번?
    COUNT(*) AS frequency,
    -- M(수익): ROI 총합
    ROUND(SUM(ROI), 2) AS monetary
  FROM lexus_ad.campaign_adjusted
  GROUP BY Company, Channel_Used   -- 5회사 × 6채널 = 30개 조합
),

-- 2단계: 각 지표를 5등분해서 1~5점 매기기
rfm_scored AS (
  SELECT *,
    -- NTILE(5) = 30개를 정렬해서 5묶음으로 나누고 1~5번호 붙이기
    NTILE(5) OVER (ORDER BY recency DESC) AS r_score,    -- 최근일수록 높은 점수
    NTILE(5) OVER (ORDER BY frequency ASC) AS f_score,   -- 자주 할수록 높은 점수
    NTILE(5) OVER (ORDER BY monetary ASC) AS m_score     -- 수익 높을수록 높은 점수
  FROM rfm_raw
)

-- 3단계: 점수 합산해서 등급 분류
SELECT *,
  ROUND((r_score + f_score + m_score) / 3.0, 1) AS rfm_avg,
  CASE
    WHEN (r_score + f_score + m_score) >= 12 THEN 'VIP'      -- 12~15점
    WHEN (r_score + f_score + m_score) >= 9 THEN 'Loyal'     -- 9~11점
    WHEN (r_score + f_score + m_score) >= 6 THEN 'Normal'    -- 6~8점
    ELSE 'At Risk'                                             -- 3~5점
  END AS segment
FROM rfm_scored
```

### 읽는 순서
1. `WITH rfm_raw AS (...)` → 먼저 R, F, M 값 계산
2. `rfm_scored AS (...)` → 그 결과로 1~5점 매기기
3. 마지막 `SELECT` → 점수 합산해서 VIP~At Risk 등급 붙이기

### 핵심 함수
- `DATE_DIFF(날짜1, 날짜2, DAY)` → 두 날짜 사이 일수
- `MAX(DATE(Date))` → 가장 최근 날짜
- `NTILE(5) OVER (ORDER BY ...)` → 정렬 후 5등분
- `WITH ... AS` → CTE(임시 테이블). 단계별로 나눠서 읽기 쉽게

---

## 6단계: 윈도우 함수 분석

마트 데이터로 추세, 성장률, 순위를 분석.

### 월별 성장률 + 누적 + 이동평균

```sql
CREATE OR REPLACE TABLE lexus_ad.mart_monthly_growth AS
SELECT
  month,
  avg_roi,
  total_clicks,
  total_impressions,

  -- 전월 대비 ROI 변화량
  -- LAG = "이전 행 값 가져오기"
  ROUND(avg_roi - LAG(avg_roi) OVER (ORDER BY month), 2) AS roi_change,
  -- 예: 6월 ROI 6.0 - 5월 ROI 5.5 = 0.5

  -- 전월 대비 성장률(%)
  ROUND(
    (avg_roi - LAG(avg_roi) OVER (ORDER BY month))
    / LAG(avg_roi) OVER (ORDER BY month) * 100
  , 1) AS roi_growth_pct,
  -- 예: 0.5 / 5.5 * 100 = 9.1%

  -- 누적 클릭수: 1월부터 현재 월까지 합계
  SUM(total_clicks) OVER (ORDER BY month) AS cumulative_clicks,
  -- 예: 1월 100 → 2월 100+90=190 → 3월 190+110=300

  -- 누적 노출수
  SUM(total_impressions) OVER (ORDER BY month) AS cumulative_impressions,

  -- 3개월 이동평균: 최근 3개월 ROI 평균
  ROUND(AVG(avg_roi) OVER (
    ORDER BY month
    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW  -- 현재 포함 이전 2개 = 3개
  ), 2) AS moving_avg_3m
  -- 예: 3월 이동평균 = (1월 + 2월 + 3월) / 3

FROM lexus_ad.mart_monthly
ORDER BY month
```

### 핵심 함수
- `LAG(컬럼) OVER (ORDER BY ...)` → 이전 행의 값
- `SUM(...) OVER (ORDER BY ...)` → 누적 합계
- `AVG(...) OVER (ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)` → 이동 평균

### 월별 채널 순위

```sql
CREATE OR REPLACE TABLE lexus_ad.mart_channel_rank AS

-- 먼저 월별 × 채널별 평균 ROI 계산
WITH channel_monthly AS (
  SELECT
    FORMAT_DATE("%Y-%m", Date) AS month,
    Channel_Used AS channel,
    ROUND(AVG(ROI), 2) AS avg_roi
  FROM lexus_ad.campaign_adjusted
  GROUP BY month, channel
)

-- 매달 ROI 높은 순으로 순위 매기기
SELECT
  month,
  channel,
  avg_roi,
  RANK() OVER (
    PARTITION BY month        -- 월별로 나눠서
    ORDER BY avg_roi DESC     -- ROI 높은 순으로
  ) AS rank                   -- 순위 매기기
FROM channel_monthly
ORDER BY month, rank
```

### 핵심 함수
- `RANK() OVER (PARTITION BY month ORDER BY avg_roi DESC)`
  - `PARTITION BY month` → 월별로 따로 순위 매김 (GROUP BY와 비슷하지만 행을 줄이지 않음)
  - `ORDER BY avg_roi DESC` → ROI 높은 순
  - `RANK()` → 1등, 2등, 3등...

---

## 7단계: 분석 쿼리 (마트에서 데이터 꺼내기)

마트를 만들어뒀으니까 간단한 SELECT로 인사이트를 뽑음.

### 채널별 ROI 비교

```sql
SELECT channel, avg_roi, avg_conversion_rate
FROM lexus_ad.mart_channel
ORDER BY avg_roi DESC
```

### 월별 ROI 추이

```sql
SELECT month, avg_roi
FROM lexus_ad.mart_monthly
ORDER BY month
```

### 지역별 성과

```sql
SELECT location, avg_roi
FROM lexus_ad.mart_location
ORDER BY avg_roi DESC
```

### RFM 등급별 결과

```sql
SELECT Company, channel, total_roi, segment
FROM lexus_ad.mart_rfm
ORDER BY total_roi DESC
```

### 채널별 CTR·전환율·ROI

```sql
SELECT channel, ctr_pct AS CTR, avg_conversion_rate AS conversion_rate, avg_roi AS ROI
FROM lexus_ad.mart_ctr_engagement
ORDER BY avg_roi DESC
```

---

## 전체 흐름 요약

```
1. 업로드     campaign_raw (원본 20만 행)
      ↓
2. 보정       campaign_adjusted (가중치 적용)
      ↓
3. 파티션     campaign_partitioned (비용 최적화)
      ↓
4. 마트       mart_monthly, mart_channel 등 (요약 테이블)
      ↓
5. RFM       mart_rfm (등급 분류)
      ↓
6. 윈도우     mart_monthly_growth, mart_channel_rank (추세/순위)
      ↓
7. 분석       SELECT로 마트에서 인사이트 뽑기
      ↓
8. 대시보드   Metabase에서 시각화
      ↓
9. 보고서     결론 + 제안
```
