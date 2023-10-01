import json
import csv
import requests

API_KEY = "f38c94b357eae713857038d2f1a912cc"


def get_geo_codes(city_name, limit=1):
    try:
        url = f"http://api.openweathermap.org/geo/1.0/direct?q={city_name}&limit={limit}&appid={API_KEY}"
        response = requests.get(url=url)
        data = json.loads(response.content)
        if data is not None:
            return data[0].get("lat"), data[0].get("lon")
    except Exception as e:
        print(e.__cause__)


"""
sample response: 
{
    'coord': {
        'lon': 78.4741, 
        'lat': 17.3606
    }, 
    'weather': [
        {
            'id': 721, 
            'main': 'Haze', 
            'description': 'haze', 
            'icon': '50d'
        }
    ], 
    'base': 'stations', 
    'main': {
        'temp': 303.34, 
        'feels_like': 306.77, 
        'temp_min': 301.83, 
        'temp_max': 303.34, 
        'pressure': 1007, 
        'humidity': 62
    }, 
    'visibility': 6000, 
    'wind': {
        'speed': 6.17, 
        'deg': 230
    }, 
    'clouds': {
        'all': 40
    }, 
    'dt': 1696155605, 
    'sys': {
        'type': 1, 
        'id': 9214, 
        'country': 'IN', 
        'sunrise': 1696120569, 
        'sunset': 1696163740
    }, 
    'timezone': 19800, 
    'id': 1269843, 
    'name': 'Hyderabad', 
    'cod': 200
}
"""


def get_weather_data(city_name):
    try:
        (lat, lon) = get_geo_codes(city_name=city_name)
        url = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={API_KEY}"
        response = requests.get(url=url)
        weather_data = json.loads(response.content)
        if weather_data is not None:
            print(weather_data)
            create_csv_file(city_name=city_name, data=weather_data)
    except Exception as e:
        print(e.__cause__)


def create_csv_file(city_name, data):
    # field names
    fields = ['Name', 'lat', 'lon', 'weather_desc', 'temperature', 'feels_like', 'temp_min', 'temp_max', 'pressure',
              'humidity', 'visibility', 'wind_speed', 'sunrise', 'sunset']
    # data rows of csv file
    row = [str(data.get("name")), str(data.get("coord").get("lat")), str(data.get("coord").get("lon")),
           str(data.get("weather")[0].get("description")), str(data.get("main").get("temp")), str(data.get("main").get("feels_like")),
           str(data.get("main").get("temp_min")), str(data.get("main").get("temp_max")), str(data.get("main").get("pressure")),
           str(data.get("main").get("humidity")), str(data.get("visibility")), str(data.get("wind").get("speed")),
           str(data.get("sys").get("sunrise")), str(data.get("sys").get("sunset"))]

    # name of csv file
    filename = f"weather_records.csv"

    # save the data
    with open(filename, 'w', encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
        writer.writerow(fields)
        writer.writerow(row)
        csvfile.flush()
        csvfile.close()


if __name__ == "__main__":
    get_weather_data('Hyderabad, Telangana, India')
