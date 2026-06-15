package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"

	"github.com/joho/godotenv"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	yc "github.com/ydb-platform/ydb-go-yc"
)

const (
	ydbEndpoint = "grpcs://ydb.serverless.yandexcloud.net:2135/?database=/ru-central1/b1gf8bafihir7mh02q7n/etnv771sit5jtht2dsaa"
	csvPath     = "../../data/Airbnb_Open_Data.csv"
	batchSize   = 1000
)

func main() {
	ctx := context.Background()

	if err := godotenv.Load(); err != nil {
		log.Println("Файл .env не найден, используем переменные окружения")
	}

	saKeyPath := os.Getenv("SA_KEY_PATH")
	if saKeyPath == "" {
		log.Fatal("SA_KEY_PATH не задан в .env")
	}

	db, err := ydb.Open(ctx, ydbEndpoint,
		yc.WithInternalCA(),
		yc.WithServiceAccountKeyFileCredentials(saKeyPath),
	)
	if err != nil {
		log.Fatalf("Ошибка подключения к YDB: %v", err)
	}
	defer db.Close(ctx)
	fmt.Println("Подключились к YDB")

	f, err := os.Open(csvPath)
	if err != nil {
		log.Fatalf("Ошибка открытия файла: %v", err)
	}
	defer f.Close()

	reader := csv.NewReader(f)
	reader.LazyQuotes = true
	reader.FieldsPerRecord = -1

	// Пропускаем заголовок
	if _, err := reader.Read(); err != nil {
		log.Fatalf("Ошибка чтения заголовка: %v", err)
	}

	var rows []types.Value
	total := 0

	flush := func() {
		if len(rows) == 0 {
			return
		}
		err := db.Table().Do(ctx, func(ctx context.Context, s table.Session) error {
			_, _, err := s.Execute(ctx,
				table.DefaultTxControl(),
				`DECLARE $rows AS List<Struct<
					id:Uint64,
					name:Utf8,
					host_id:Utf8,
					host_identity_verified:Utf8,
					host_name:Utf8,
					neighbourhood_group:Utf8,
					neighbourhood:Utf8,
					lat:Utf8,
					lon:Utf8,
					country:Utf8,
					country_code:Utf8,
					instant_bookable:Utf8,
					cancellation_policy:Utf8,
					room_type:Utf8,
					construction_year:Utf8,
					price:Utf8,
					service_fee:Utf8,
					minimum_nights:Utf8,
					number_of_reviews:Utf8,
					last_review:Utf8,
					reviews_per_month:Utf8,
					review_rate_number:Utf8,
					calculated_host_listings_count:Utf8,
					availability_365:Utf8,
					house_rules:Utf8,
					license:Utf8
				>>;
				UPSERT INTO airbnb SELECT
					id, name, host_id, host_identity_verified, host_name,
					neighbourhood_group, neighbourhood, lat, lon, country,
					country_code, instant_bookable, cancellation_policy, room_type,
					construction_year, price, service_fee, minimum_nights,
					number_of_reviews, last_review, reviews_per_month,
					review_rate_number, calculated_host_listings_count,
					availability_365, house_rules, license
				FROM AS_TABLE($rows);`,
				table.NewQueryParameters(
					table.ValueParam("$rows", types.ListValue(rows...)),
				),
			)
			return err
		})
		if err != nil {
			log.Fatalf("Ошибка вставки батча: %v", err)
		}
		total += len(rows)
		fmt.Printf("Вставлено строк: %d\n", total)
		rows = rows[:0]
	}

	lineNum := 0
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Пропускаем строку %d: %v", lineNum, err)
			continue
		}
		lineNum++

		// Дополняем до 26 полей если не хватает
		for len(record) < 26 {
			record = append(record, "")
		}

		id, err := strconv.ParseUint(record[0], 10, 64)
		if err != nil {
			log.Printf("Пропускаем строку %d: некорректный id %q", lineNum, record[0])
			continue
		}

		rows = append(rows, types.StructValue(
			types.StructFieldValue("id", types.Uint64Value(id)),
			types.StructFieldValue("name", types.UTF8Value(record[1])),
			types.StructFieldValue("host_id", types.UTF8Value(record[2])),
			types.StructFieldValue("host_identity_verified", types.UTF8Value(record[3])),
			types.StructFieldValue("host_name", types.UTF8Value(record[4])),
			types.StructFieldValue("neighbourhood_group", types.UTF8Value(record[5])),
			types.StructFieldValue("neighbourhood", types.UTF8Value(record[6])),
			types.StructFieldValue("lat", types.UTF8Value(record[7])),
			types.StructFieldValue("lon", types.UTF8Value(record[8])),
			types.StructFieldValue("country", types.UTF8Value(record[9])),
			types.StructFieldValue("country_code", types.UTF8Value(record[10])),
			types.StructFieldValue("instant_bookable", types.UTF8Value(record[11])),
			types.StructFieldValue("cancellation_policy", types.UTF8Value(record[12])),
			types.StructFieldValue("room_type", types.UTF8Value(record[13])),
			types.StructFieldValue("construction_year", types.UTF8Value(record[14])),
			types.StructFieldValue("price", types.UTF8Value(record[15])),
			types.StructFieldValue("service_fee", types.UTF8Value(record[16])),
			types.StructFieldValue("minimum_nights", types.UTF8Value(record[17])),
			types.StructFieldValue("number_of_reviews", types.UTF8Value(record[18])),
			types.StructFieldValue("last_review", types.UTF8Value(record[19])),
			types.StructFieldValue("reviews_per_month", types.UTF8Value(record[20])),
			types.StructFieldValue("review_rate_number", types.UTF8Value(record[21])),
			types.StructFieldValue("calculated_host_listings_count", types.UTF8Value(record[22])),
			types.StructFieldValue("availability_365", types.UTF8Value(record[23])),
			types.StructFieldValue("house_rules", types.UTF8Value(record[24])),
			types.StructFieldValue("license", types.UTF8Value(record[25])),
		))

		if len(rows) >= batchSize {
			flush()
		}
	}
	flush()

	fmt.Printf("Импорт завершён. Всего вставлено: %d строк\n", total)
}
