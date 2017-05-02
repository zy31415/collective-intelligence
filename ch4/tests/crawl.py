import bs4
import requests


url = 'https://en.wikipedia.org/wiki/Transhumanism'
content = requests.get(url).content

soup = bs4.BeautifulSoup(content, 'lxml')

body = soup.find('div', {'id': 'bodyContent'})

p_tags = body.findAll('p')

print(p_tags)

# for p_tag in p_tags:
#     print(p_tag.text)


# links = tag.findAll('a')
#
# for link in links:
#     print(link.get('href'))