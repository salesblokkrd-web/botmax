# maxbot — бот для Max мессенджера (Архиповский карьер)

## О проекте
Порт бота с Telegram на Max (мессенджер от VK).
Та же логика: приём заявок на щебень, песок, гравий, отсев, ГПС.

## Стек
- **max-botapi-python** — официальный Python SDK для Max
- **Groq API** — llama-3.3-70b (парсинг заявок) + whisper (голосовые)
- **Yandex Routing API / OSRM** — расчёт расстояния доставки
- **Amvera** — хостинг (отдельное приложение от tg-bot)

## Структура
- `bot.py` — основной код бота
- `requirements.txt` — зависимости
- `amvera.yaml` — конфиг хостинга
- `manager_id.txt` — Telegram ID менеджера (Илья: 5125266066)
- `config.json` — токен бота (НЕ коммитить!)

## Регистрация бота в Max
1. Скачать Max на телефон
2. Найти @MasterBot в поиске
3. Написать /newbot → следовать инструкциям
4. Получить ACCESS_TOKEN
5. Записать токен в переменную окружения MAX_BOT_TOKEN на Amvera

## ConversationHandler: состояния
PRODUCT → VOLUME → DELIVERY → ADDRESS → CONTACTS → PHONE_ONLY
(идентично tg-bot)

## Насыпная плотность (перевод кубов в тонны)
Коэффициенты из прайса карьера. Используются в `DENSITY` в bot.py:
- Отсев 0-5: 1.27 т/м³
- Щебень 5-20: 1.45 т/м³
- Щебень 20-40: 1.42 т/м³
- Щебень 40-70: 1.44 т/м³
- Песок мелкозернистый: 1.50 т/м³
- Песок крупнозернистый: 1.50 т/м³
- Гравий: 1.45 т/м³
- ГПС плохой: 1.77 т/м³
- ГПС хороший: 1.77 т/м³
- DEFAULT_DENSITY (неизвестный продукт): 1.5 т/м³

## Security rules
- NEVER commit credentials or API keys
- `config.json` может содержать токены — NEVER commit
- NEVER commit `.env` files
- ALWAYS run `git status` перед коммитом

## Деплой
- `git push amvera main:master` (отдельное приложение на Amvera)
