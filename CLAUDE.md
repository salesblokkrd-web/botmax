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

## Security rules
- NEVER commit credentials or API keys
- `config.json` может содержать токены — NEVER commit
- NEVER commit `.env` files
- ALWAYS run `git status` перед коммитом

## Деплой
- `git push amvera main:master` (отдельное приложение на Amvera)
