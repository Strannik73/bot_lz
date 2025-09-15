import openmeteo_requests
from datetime import datetime
import pandas as pd
import requests_cache
import requests
from retry_requests import retry

import asyncio
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import  BotCommand 
from tok import token_r
import pandas as pd


#=======================ТОКЕН==============
token_r = ''

bot = Bot(token_r)
dp = Dispatcher()
#==================================================================================
data = {'@TG_nick': [], 'Motion': [], 'API': [], 'Date': [], 'Time': [], 'API_answer': []}
df = pd.DataFrame(data)

def logger(func):
    async def wrapper(*args):
        func_return = await func(*args)
        message = args[0]
        if len(func_return) == 3:
            motion = func_return[0]
            api = func_return[1]
            api_answer = func_return[2]
        else:
            motion = func_return[0]
            api = 'None'
            api_answer = 'None'
        df.loc[len(df.index)] = [message.from_user.username, motion, api, datetime.now().date(), datetime.now().time(), api_answer]
        df.to_csv('log.csv', index=True, index_label='Unic_ID')
    return wrapper


#=================================================================================
urlmk = "https://api.open-meteo.com/v1/forecast?latitude=53.9&longitude=27.5667&hourly=temperature_2m,precipitation_probability&forecast_days=3"
urlv = "https://api.open-meteo.com/v1/forecast?latitude=55.1904&longitude=30.2049&hourly=temperature_2m,rain,apparent_temperature&timezone=Europe%2FMoscow&forecast_days=3"
urlmg = "https://api.open-meteo.com/v1/forecast?latitude=53.9168&longitude=30.3449&hourly=temperature_2m,rain,apparent_temperature&timezone=Europe%2FMoscow&forecast_days=3"

async def set_commands():
    commands = [
        BotCommand(command="start", description="Запустить бота"),
    ]
    await bot.set_my_commands(commands)


#start
@dp.message(Command("start"))
@logger
async def cmd_start(message: types.Message):
    keyboard = types.ReplyKeyboardMarkup(keyboard=POGODA, resize_keyboard=True)
    await message.answer("Я бот на aiogram. выбери что сделать", reply_markup=keyboard)
    motion = 'Press start btn'
    return [motion]
#~~~~~~КНОПКИ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)

POGODA = [[types.KeyboardButton(text="Минск")],
        [types.KeyboardButton(text="Витебск")],
        [types.KeyboardButton(text="Могилев")],
        ]


@dp.message(F.text == "Минск")
@logger
async def handle_minsk(message: types.Message):
    await message.answer("Запрашиваю погоду для Минска…")

    url = urlmk
    params = {
        "latitude": 53.9,
        "longitude": 27.5667,
        "hourly": ["temperature_2m", "rain", "apparent_temperature"],
        "timezone": "Europe/Moscow",
        "forecast_days": 1,
    }

    responses = openmeteo.weather_api(url, params=params)
    response = responses[0]

    coords = f"{response.Latitude()}°N {response.Longitude()}°E"
    elevation = f"{response.Elevation()} m"
    tz       = f"{response.Timezone()} ({response.TimezoneAbbreviation()})"

    hourly = response.Hourly()
    times = pd.date_range(
        start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
        end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
        freq=pd.Timedelta(seconds=hourly.Interval()),
        inclusive="left"
    )
    temp_vals  = hourly.Variables(0).ValuesAsNumpy()
    rain_vals  = hourly.Variables(1).ValuesAsNumpy()
    app_temp   = hourly.Variables(2).ValuesAsNumpy()
    
    df = pd.DataFrame({
        "time": times,
        "temp_2m": temp_vals,
        "rain": rain_vals,
        "feels_like": app_temp
    })

    header = (
        f"Погода в Минске\n"
        f"Координаты: {coords}\n"
        f"Высота над уровнем моря: {elevation}\n"
        f"Часовой пояс: {tz}"
    )

    table = df.head().to_string(index=False)

    await message.answer(f"{header}\n\nHourly data (first 5 rows):\n{table}")
    code_resp = requests.get(url)
    motion = 'Press minsk btn'
    return [motion, url, code_resp]
#=========================================================================================================

@dp.message(F.text == "Витебск")
@logger
async def handle_vitebsk(message: types.Message):
    await message.answer("Запрашиваю погоду для Витебска…")

    url = urlv
    params = {
        "latitude": 55.1904,
        "longitude": 30.2049,
        "hourly": ["temperature_2m", "rain", "apparent_temperature"],
        "timezone": "Europe/Moscow",
        "forecast_days": 1,
    }

    response = openmeteo.weather_api(url, params=params)[0]

    coords    = f"{response.Latitude()}°N {response.Longitude()}°E"
    elevation = f"{response.Elevation()} m"
    tz        = f"{response.Timezone()} ({response.TimezoneAbbreviation()})"

    hourly      = response.Hourly()
    times       = pd.date_range(
        start=pd.to_datetime(hourly.Time(),    unit="s", utc=True),
        end  =pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
        freq =pd.Timedelta(seconds=hourly.Interval()),
        inclusive="left"
    )
    temp_vals   = hourly.Variables(0).ValuesAsNumpy()
    rain_vals   = hourly.Variables(1).ValuesAsNumpy()
    feel_vals   = hourly.Variables(2).ValuesAsNumpy()

    df = pd.DataFrame({
        "time":             times.tz_convert("Europe/Moscow"),
        "temp_2m (℃)":      temp_vals,
        "rain (mm)":        rain_vals,
        "feels_like (℃)":   feel_vals,
    })

    header = (
        f"Погода в Витебске\n"
        f"Координаты: {coords}\n"
        f"Высота над уровнем моря: {elevation}\n"
        f"Часовой пояс: {tz}"
    )
    table = df.head().to_string(index=False)

    await message.answer(f"{header}\n\nHourly data (first 5 rows):\n{table}") 
    code_resp = requests.get(url)
    motion = 'Press vitebsk btn'
    return [motion, url, code_resp]                
#=====================================================================================================

@dp.message(F.text == "Могилев")
@logger
async def handle_mogilev(message: types.Message):
    await message.answer("Запрашиваю погоду для Могилева…")

    url = urlmg
    params = {
        "latitude": 53.9168,
        "longitude": 30.3449,
        "hourly": ["temperature_2m", "rain", "apparent_temperature"],
        "timezone": "Europe/Moscow",
        "forecast_days": 1,
    }

    response = openmeteo.weather_api(url, params=params)[0]

    coords    = f"{response.Latitude()}°N {response.Longitude()}°E"
    elevation = f"{response.Elevation()} m"
    tz        = f"{response.Timezone()} ({response.TimezoneAbbreviation()})"

    hourly      = response.Hourly()
    times       = pd.date_range(
        start=pd.to_datetime(hourly.Time(),    unit="s", utc=True),
        end  =pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
        freq =pd.Timedelta(seconds=hourly.Interval()),
        inclusive="left"
    ).tz_convert("Europe/Moscow")
    temp_vals   = hourly.Variables(0).ValuesAsNumpy()
    rain_vals   = hourly.Variables(1).ValuesAsNumpy()
    feel_vals   = hourly.Variables(2).ValuesAsNumpy()

    df = pd.DataFrame({
        "time":               times,
        "temp_2m (℃)":        temp_vals,
        "rain (mm)":          rain_vals,
        "feels_like (℃)":     feel_vals,
    })

    header = (
        f"Погода в Могилеве\n"
        f"Координаты: {coords}\n"
        f"Высота над уровнем моря: {elevation}\n"
        f"Часовой пояс: {tz}"
    )
    table = df.head().to_string(index=False)

    await message.answer(f"{header}\n\nПочасовые данные (первые 5 строк):\n{table}")
    code_resp = requests.get(url)
    motion = 'Press mogilev btn'
    return [motion, url, code_resp]
    
    
# Обработчик сообщения
@dp.message()
@logger
async def echo(message: types.Message):
    await message.answer(f"Ты написал: {message.text}")
    motion = 'Keyboard typing'
    return [motion]


#==========MAIN===========
async def main():
    print('ЗАГРУЗКА БОТА...')
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())