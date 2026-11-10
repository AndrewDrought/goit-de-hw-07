# Підрахунок олімпійських медалей в Apache Airflow

DAG `zasukha_medal_counts` випадково вибирає медаль, рахує відповідні рядки в MySQL та перевіряє свіжість записаного результату. Вихідний код міститься в [zasukha_medal_counts.py](zasukha_medal_counts.py), результати запусків і скриншоти Airflow та MySQL — у [звіті](report.docx).

## Як працює DAG

1. `create_table` створює таблицю `zasukha_medal_counts` через `CREATE TABLE IF NOT EXISTS`. Поле `id` є первинним ключем з автоінкрементом.
2. `pick_medal` виконує `random.choice` для Bronze, Silver або Gold. Airflow записує повернене значення в XCom.
3. `choose_branch` читає його з XCom і повертає ID відповідної задачі підрахунку.
4. Одна з трьох SQL-задач виконує `INSERT … SELECT COUNT(*)` для обраної медалі. В одному рядку зберігаються тип медалі, кількість записів і час MySQL у UTC. Дві інші гілки отримують статус `skipped`.
5. `delay_check` використовує правило `none_failed_min_one_success` і виконує `time.sleep(delay_seconds)` після успішної гілки.
6. `check_latest_record` перевіряє, чи лежить `MAX(created_at)` у проміжку від поточного часу MySQL мінус 30 секунд до поточного часу включно. Для порожньої таблиці запит повертає 0. Сенсор повторює перевірку кожні 2 секунди, а після тайм-ауту 6 секунд завершується зі статусом `failed`. DAG виконує щонайбільше один активний запуск, щоб наступний запуск не оновив таблицю під час цієї перевірки.

## Відтворення

Перевірене середовище використовує Apache Airflow 2.10.5, Python 3.12 і MySQL 8.4.7. У середовищі Airflow мають бути встановлені провайдери з [requirements.txt](requirements.txt).

У Airflow налаштуйте MySQL Connection `goit_mysql_db`. У полі Schema вкажіть наявну базу для таблиці результатів. Користувачу потрібні права створення таблиці, читання та вставки в ній і право читання `olympic_dataset.athlete_event_results`. Для локального відтворення вихідна таблиця містить записи з [CSV курсу GoIT](https://ftp.goit.study/neoversity/athlete_event_results.csv), зокрема стовпець `medal`.

Скопіюйте файл DAG у каталог DAGs і запустіть його вручну. У формі Trigger DAG параметр `delay_seconds` за замовчуванням дорівнює 2. Для перевірки застарілого запису задайте 35. Команди для обох сценаріїв:

```bash
airflow dags trigger zasukha_medal_counts --conf '{"delay_seconds": 2}'
airflow dags trigger zasukha_medal_counts --conf '{"delay_seconds": 35}'
```

У Grid та Graph видно обрану гілку, дві пропущені задачі й стан сенсора. Для звірки кількостей виконайте [verify.sql](verify.sql) у базі результатів. У вихідному CSV 316 834 рядки, серед яких 14 939 мають Bronze, 14 676 — Silver і 15 072 — Gold.

Демонстраційні запуски виконано локально через `DAG.test()` із MySQL та зі збереженням станів задач у метабазі Airflow. Із 13 запусків 12 завершилися успішно, а один із затримкою 35 секунд — зі статусом `failed` на сенсорі.
