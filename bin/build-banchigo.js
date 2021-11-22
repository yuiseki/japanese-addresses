const fs = require('fs');
const readline = require('readline')
const path = require('path')
const fetch = require('isomorphic-unfetch');
const { JSDOM } = require('jsdom');
const url = require('url')

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms))

const reafFileStream = fs.createReadStream(path.resolve(__dirname, '..', 'data', 'latest.csv'))
const cityCodeMap = new Map()

readline
  .createInterface({ input: reafFileStream })
  .on('line', (line) => {
    const [,quotedPrefName,,,quotedCityCode, quotedCityName] = line.split(',')
    const prefName = quotedPrefName.replace(/"/g, '')
    const cityCode = quotedCityCode.replace(/"/g, '')
    const cityName = quotedCityName.replace(/"/g, '')
    if(!cityCodeMap.has(cityCode)) {
      if(
        // cityCode === '01452' ||
        // cityCode === '01455' ||
        // cityCode === '25207' ||
        cityCode === '44203' ||
        cityCode === '47325'

      ) { // TODO: debug
        cityCodeMap.set(cityCode, { prefName, cityName })
      }
    }
  } )
  .on('close',async () => {
    const urlFormat = 'https://saigai.gsi.go.jp/jusho/view/pref/city/%code.html'

    for (const [cityCode, { prefName, cityName }] of cityCodeMap) {
      const cityUrl = urlFormat.replace('%code', cityCode)
      console.log(cityUrl)
      const resp = await fetch(cityUrl)

      if (resp.status === 200) {
        const { window } = new JSDOM(await resp.text())
        const anchors = window.document.getElementsByTagName('a')
        const cyomokuURLs = [...anchors]
          .map(anchor => anchor.getAttribute('href'))
          .filter(link => link.match(new RegExp(`data/${cityCode}/${cityCode}_[0-9]+\.html`)))
          .map(relativeURL => url.resolve(cityUrl, relativeURL))

        const banchigoItemsMap = {}
        while (cyomokuURLs.length > 0) {
          const cyomokuURL = cyomokuURLs.shift()
          console.log(cyomokuURL)
          const resp = await fetch(cyomokuURL)
          const { window } = new JSDOM(await resp.text())
          const table = window.document.getElementsByTagName('table')[0]
          const [header, ...rows] = table.querySelectorAll('tr')
          const headerTexts = [...header.querySelectorAll('th')].map(th => th.textContent)

          for (const row of rows) {
            const cols = [...row.querySelectorAll('td')]
            const values = cols.map(td => td.textContent)
            const cyomokuName = values[headerTexts.indexOf('町又は字の名称')]
            if(!banchigoItemsMap[cyomokuName]) {
              banchigoItemsMap[cyomokuName] = []
            }
            banchigoItemsMap[cyomokuName].push(values.reduce((prev, value, index) => {
              prev[headerTexts[index]] = value
              return prev
            }, {}))
          }
          await sleep(500)
        }

        const cityDirName = path.resolve(__dirname, '..', 'api', 'ja', prefName, cityName)
        fs.mkdirSync(cityDirName, { recursive: true })

        const cyomokuNames = Object.keys(banchigoItemsMap)
        for (const cyomokuName of cyomokuNames) {
          const banchigoFilename = path.resolve(cityDirName, cyomokuName + '.json')
          const data = banchigoItemsMap[cyomokuName].map(item => ({
            gaiku: item['街区符号'],
            kiso: item['基礎番号'],
            lat: item['緯度(度単位10進数)'],
            lng: item['経度(度単位10進数)'],
          }))
          fs.writeFileSync(banchigoFilename, JSON.stringify(data))
        }
      }
      cityCodeMap.set(cityCode, { prefName, cityName, status: resp.status })
      await sleep(1000)
    }

    fs.writeFileSync(path.resolve(__dirname, 'result.json'), JSON.stringify(Object.fromEntries(cityCodeMap)))
  })
