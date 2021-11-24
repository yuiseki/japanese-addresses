const fs = require('fs');
const readline = require('readline')
const path = require('path')
const fetch = require('isomorphic-unfetch');
const { JSDOM } = require('jsdom');
const url = require('url')

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms))

const reafFileStream = fs.createReadStream(path.resolve(__dirname, '..', 'data', 'latest.csv'))
const csvResultFd = fs.openSync(path.resolve(__dirname, '..', 'data', 'banchi-go.csv'), 'a')
const logFd = fs.openSync(path.resolve(__dirname, '..', 'banchi-go.log'), 'a')
const log = (message) => {
  const now = new Date().toISOString()
  fs.writeFileSync(logFd, `[${now}] ${message}`)
}

const cityCodeMap = new Map()

readline
  .createInterface({ input: reafFileStream })
  .on('line', (line) => {
    const [quotedPrefCode, quotedPrefName,,,quotedCityCode, quotedCityName] = line.split(',')
    const prefCode = quotedPrefCode.replace(/"/g, '')
    const prefName = quotedPrefName.replace(/"/g, '')
    const cityCode = quotedCityCode.replace(/"/g, '')
    const cityName = quotedCityName.replace(/"/g, '')
    if(!cityCodeMap.has(cityCode)) {
      cityCodeMap.set(cityCode, { prefName, cityName })
    }
  } )
  .on('close',async () => {
    const urlFormat = 'https://saigai.gsi.go.jp/jusho/view/pref/city/%code.html'

    for (const [cityCode] of cityCodeMap) {
      const cityURL = urlFormat.replace('%code', cityCode)
      let resp
      try {
        resp = await fetch(cityURL)
        if(resp.status > 399) {
          throw new Error(`Request failed with ${resp.status}`)
        }
      } catch (error) {
        log(`Request to ${cityURL} failed with ${JSON.stringify(error)}`)
        continue
      }
      log(`Request to ${cityURL} succeeded.`)

      if (resp.status === 200) {
        const { window } = new JSDOM(await resp.text())
        const anchors = window.document.getElementsByTagName('a')
        const cyomokuURLs = [...anchors]
          .map(anchor => anchor.getAttribute('href'))
          .filter(link => link.match(new RegExp(`data/${cityCode}/${cityCode}_[0-9]+\.html`)))
          .map(relativeURL => url.resolve(cityURL, relativeURL))

        const banchigoItemsMap = {}
        while (cyomokuURLs.length > 0) {
          const cyomokuURL = cyomokuURLs.shift()
          let resp
          try {
            resp = await fetch(cyomokuURL)
            if(resp.status > 399) {
              throw new Error(`Request failed with ${resp.status}`)
            }
          } catch (error) {
            log(`Request to ${cyomokuURL} failed with ${JSON.stringify(error)}`)
            continue
          }
          log(`Request to ${cyomokuURL} succeeded.`)

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
            const csvLine = values.join(',')
            fs.writeFileSync(csvResultFd, csvLine + '\n')
          }
          await sleep(300)
        }
      }
      await sleep(1000)
    }

    fs.closeSync(csvResultFd)
    fs.closeSync(logFd)
  })
