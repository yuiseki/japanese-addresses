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
      cityCodeMap.set(cityCode, { prefName, cityName })
    }
  } )
  .on('close',async () => {
    const urlFormat = 'https://saigai.gsi.go.jp/jusho/view/pref/city/%code.html'

    for (const [cityCode, { prefName, cityName }] of cityCodeMap) {
      const cityUrl = urlFormat.replace('%code', cityCode)
      const resp = await fetch(cityUrl)
      let cyomokuCount = 0
        if(resp.status === 200) {
        const { window } = new JSDOM(await resp.text())
        const anchors = window.document.getElementsByTagName('a')
        const cyomokuURLs = [...anchors]
          .map(anchor => anchor.getAttribute('href'))
          .filter(link => link.match(new RegExp(`data/${cityCode}/${cityCode}_[0-9]+\.html`)))
          .map(relativeURL => url.resolve(cityUrl, relativeURL))

        const banchigoItems = []
        while (cyomokuURLs.length > 0) {
          const cyomokuURL = cyomokuURLs.shift()
          console.log(cyomokuURL)
          const resp = await fetch(cyomokuURL)
          const { window } = new JSDOM(await resp.text())
          const table = window.document.getElementsByTagName('table')[0]
          const [header, ...rows] = table.querySelectorAll('tr')
          const headerTexts = [...header.querySelectorAll('th')].map(th => th.textContent)
          cyomokuCount = rows.length

          for (const row of rows) {
            const cols = [...row.querySelectorAll('td')]
            const values = cols.map(td => td.textContent)
            banchigoItems.push(values.reduce((prev, value, index) => {
              prev[headerTexts[index]] = value
              return prev
            }, {}))
          }
          await sleep(300)
        }

        const cityDirName = path.resolve(__dirname, '..', 'api', 'ja', prefName, cityName)
        console.log(banchigoItems[0]['町又は字の名称'])
        const banchigoFilename = path.resolve(cityDirName, banchigoItems[0]['町又は字の名称'] + '.json')
        fs.mkdirSync(cityDirName, { recursive: true })
        fs.writeFileSync(banchigoFilename, JSON.stringify(banchigoItems))
      }
      cityCodeMap.set(cityCode, { prefName, cityName, status: resp.status, cyomokuCount })
    }

    fs.writeFileSync(path.resolve(__dirname, 'result.json'), JSON.stringify(Object.fromEntries(cityCodeMap)))
  })
