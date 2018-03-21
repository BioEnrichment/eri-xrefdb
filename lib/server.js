

import express from 'express'
import bodyParser from 'body-parser'

import { getOrCreateERIForURIs, getURIsForERI } from './xrefdb'


const app = express()

app.use(bodyParser.json())


app.post('/eri2uris', (req, res) => {

	// have eri, want uris

    const eri = req.body.eri

    getURIsForERI(eri).then((uris) => {

        res.send(JSON.stringify({
            uris: uris
        }))

    }).catch((err) => {

        res.status(500).send(err.toString())

    })
	


})

app.post('/uris2eri', (req, res) => {

	// have uri(s), want eri

	const uris = req.body.uris
    const type = req.body.type

    getOrCreateERIForURIs(uris, type).then((eri) => {

        res.send(JSON.stringify({
            eri: eri
        }))

    }).catch((err) => {

        res.status(500).send(err.toString())

    })
	
})


app.post('/', (req, res) => {

})

app.listen(9870)

