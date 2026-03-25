# 렉서스 광고 캠페인 분석 - 기술 리포트

## 1. 프로젝트 아키텍처

```
[Kaggle 데이터셋] → [BigQuery 원본 테이블]
                         ↓ 가중치 적용
                    [보정 테이블 campaign_adjusted]
                         ↓ GROUP BY 요약
                    [데이터 마트 9개]
                         ↓
              ┌──────────┴──────────┐
         [BigQuery 콘솔]      [Metabase 대시보드]
           SQL 분석           (BigQuery 직접 연결)
                                    ↓
                              [ngrok 외부 공유]
```

---

## 2. 데이터 환경

| 항목 | 도구 |
|------|------|
| 데이터 웨어하우스 | Google BigQuery |
| Python 환경 | uv + Python 3.9 |
| 컨테이너 | Docker (Metabase) |
| 대시보드 | Metabase (BigQuery 직접 연결) |
| 외부 공유 | ngrok 터널링 |
| 버전 관리 | Git + GitHub |

---

## 3. 데이터셋

- 출처: Kaggle "Marketing Campaign Performance Dataset"
- 규모: 200,000행, 15개 컬럼
- 기간: 2021.01 ~ 2021.12
- 렉서스 클라이언트 시나리오를 적용하여 채널별 가중치 보정

### 데이터 보정 (campaign_adjusted)

원본 데이터가 균일하여 채널별 차이가 없었기 때문에, 렉서스(프리미엄 브랜드) 시나리오에 맞게 가중치를 적용함.

```sql
-- 채널별 ROI 가중치 예시
ROI * CASE Channel_Used
  WHEN 'YouTube' THEN 1.6
  WHEN 'Instagram' THEN 1.4
  WHEN 'Facebook' THEN 1.1
  WHEN 'Google Ads' THEN 1.0
  WHEN 'Website' THEN 0.7
  WHEN 'Email' THEN 0.4
END
-- 지역별, 시즌별 가중치도 추가 적용
```

---

## 4. BigQuery 테이블 구조

```
프로젝트: lexus-ad-project-2026
 └── 데이터셋: lexus_ad
      ├── campaign_adjusted         (보정 데이터 20만 행)
      ├── campaign_partitioned      (월별 파티션)
      ├── campaign_partitioned_daily (일별 파티션)
      ├── campaign_clustered        (파티션 + 클러스터링)
      ├── mart_monthly              (월별 요약)
      ├── mart_monthly_growth       (월별 성장률 + 이동평균)
      ├── mart_channel              (채널별 요약)
      ├── mart_channel_rank         (월별 채널 순위)
      ├── mart_campaign_type        (캠페인 유형별 요약)
      ├── mart_location             (지역별 요약)
      ├── mart_rfm                  (회사×채널 RFM)
      ├── mart_segment_channel      (세그먼트×채널)
      └── mart_ctr_engagement       (CTR/참여도/ROI)
```

---

## 5. 데이터 마트 설계

### 예시: mart_monthly

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

### 예시: mart_rfm (RFM 세그먼트 분석)

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
),
rfm_scored AS (
  SELECT *,
    NTILE(5) OVER (ORDER BY recency DESC) AS r_score,
    NTILE(5) OVER (ORDER BY frequency ASC) AS f_score,
    NTILE(5) OVER (ORDER BY monetary ASC) AS m_score
  FROM rfm_raw
)
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

### RFM 분석 한계점

- 30개 조합 전부 마지막 캠페인 날짜가 2021-12-31로 동일 → recency가 모두 0
- R(최근성) 점수는 NTILE로 5등분해도 값이 같아서 랜덤 배정됨 → **변별력 없음**
- 실질적으로 **F(빈도)와 M(수익)이 등급을 결정**함
- 실무에서는 캠페인 종료 시점이 다양하므로 R도 유의미할 것

### 예시: mart_monthly_growth (윈도우 함수 활용)

```sql
CREATE OR REPLACE TABLE lexus_ad.mart_monthly_growth AS
SELECT
  month,
  avg_roi,
  total_clicks,
  total_impressions,
  ROUND(avg_roi - LAG(avg_roi) OVER (ORDER BY month), 2) AS roi_change,
  ROUND((avg_roi - LAG(avg_roi) OVER (ORDER BY month)) / LAG(avg_roi) OVER (ORDER BY month) * 100, 1) AS roi_growth_pct,
  SUM(total_clicks) OVER (ORDER BY month) AS cumulative_clicks,
  SUM(total_impressions) OVER (ORDER BY month) AS cumulative_impressions,
  ROUND(AVG(avg_roi) OVER (ORDER BY month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS moving_avg_3m
FROM lexus_ad.mart_monthly
ORDER BY month
```

### 예시: mart_channel_rank (월별 채널 순위)

```sql
CREATE OR REPLACE TABLE lexus_ad.mart_channel_rank AS
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

---

## 6. 파티션 전략 및 비용 최적화

### 적용한 파티션

| 테이블 | 파티션 기준 | 설명 |
|--------|-----------|------|
| campaign_partitioned | DATE_TRUNC(Date, MONTH) | 월별 12개 파티션 |
| campaign_partitioned_daily | DATE(Date) | 일별 365개 파티션 |
| campaign_clustered | DATE(Date) + CLUSTER BY Channel_Used, Campaign_Type, Location | 파티션 + 클러스터링 |

### 파티션 생성 SQL

```sql
-- 월별 파티션
CREATE TABLE lexus_ad.campaign_partitioned
PARTITION BY DATE_TRUNC(Date, MONTH)
AS SELECT * FROM lexus_ad.campaign_raw

-- 일별 파티션
CREATE TABLE lexus_ad.campaign_partitioned_daily
PARTITION BY DATE(Date)
AS SELECT * FROM lexus_ad.campaign_raw

-- 파티션 + 클러스터링
CREATE TABLE lexus_ad.campaign_clustered
PARTITION BY DATE(Date)
CLUSTER BY Channel_Used, Campaign_Type, Location
AS SELECT * FROM lexus_ad.campaign_adjusted
```

### 비용 비교 (dry_run으로 측정)

dry_run: 쿼리를 실제 실행하지 않고 스캔량만 미리 확인하는 기능. 비용 발생 없음.

```python
job_config = bigquery.QueryJobConfig(dry_run=True)
job = client.query(query, job_config=job_config)
print(job.total_bytes_processed)
```

| 조회 범위 | 파티션 없음 | 월별 파티션 | 일별 파티션 |
|-----------|-----------|-----------|-----------|
| 하반기 (6개월) | 3.2MB | 1.8MB (-41%) | 1.8MB (-41%) |
| 4분기 (3개월) | 3.2MB | 0.8MB (-75%) | 0.8MB (-75%) |
| 12월만 (1개월) | 3.2MB | 0.3MB (-92%) | 0.3MB (-92%) |
| 특정 하루 | 3.2MB | 0.3MB (-92%) | 0.009MB (-99.7%) |

### 클러스터링

20만 행 규모에서는 클러스터링 효과가 미미했음. BigQuery는 블록 단위(수MB)로 저장하는데, 데이터가 작아 블록이 적어 차이가 안 남. 대규모 데이터(수천만 행 이상)에서 유의미한 효과 기대.

---

## 7. 인프라 구성

### Docker Compose

```yaml
services:
  metabase:
    image: metabase/metabase
    ports:
      - "3000:3000"
```

### Metabase - BigQuery 연결

서비스 계정 키(JSON)로 BigQuery에 직접 연결.

기존 프로젝트에서 서비스 계정 키 생성이 조직 정책(iam.disableServiceAccountKeyCreation)으로 차단되어, 새 프로젝트(lexus-ad-project-2026) 생성 후 해결.

### 외부 공유

ngrok으로 localhost:3000을 외부 URL로 터널링하여 대시보드 공유. 공개 링크 설정으로 로그인 없이 접근 가능.

---

## 8. 대시보드 구성 (Metabase + BigQuery)

### 현황 카드
| 카드 | 쿼리 |
|------|------|
| 총 노출수 | `SELECT SUM(Impressions) FROM lexus_ad.campaign_adjusted` |
| 총 클릭수 | `SELECT SUM(Clicks) FROM lexus_ad.campaign_adjusted` |
| 평균 전환율 | `SELECT ROUND(AVG(Conversion_Rate) * 100, 1) FROM lexus_ad.campaign_adjusted` |
| 평균 ROI | `SELECT ROUND(AVG(ROI), 2) FROM lexus_ad.campaign_adjusted` |

### 차트
| 차트 | 데이터 소스 | 시각화 |
|------|-----------|--------|
| 채널별 ROI (RFM 세그먼트) | mart_rfm | 막대 차트 (시리즈: segment) |
| 회사별 채널 ROI | mart_rfm | 막대 차트 (시리즈: channel) |
| 월별 ROI 추이 | mart_monthly | 꺾은선 차트 |
| 캠페인 유형별 전환율 | mart_campaign_type | 막대 차트 |
| 지역별 ROI | mart_location | 막대 차트 |
| 채널별 CTR·전환율·ROI | mart_ctr_engagement | 막대 차트 (Y축 3개) |
| 세그먼트별 채널 ROI | mart_segment_channel | 막대 차트 (시리즈: channel) |

---

## 9. 분석 결과 요약

비즈니스 인사이트는 [비즈니스 리포트](lexus_executive_summary.md) 참조.
SQL 코드 상세 해석은 [SQL 공부 가이드](sql_study_guide.md) 참조.
