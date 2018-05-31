
import getConfig from 'eri-config'

import request from 'request'

import monitor from 'pg-monitor'

import crypto from 'crypto'

var pgpOpts = {}
monitor.attach(pgpOpts)
const pgp = require('pg-promise')(pgpOpts)


const prefix = 'http://enrichment.ncl.ac.uk/'


const db = pgp({
    host: 'localhost',
    port: 5432,
    database: 'xrefdb',
    user: 'postgres',
    password: 'postgres'
})


const tables = {
	'DNA': 'dna',
	'RNA': 'rna',
	'Protein': 'protein',
	'SmallMolecule': 'small_molecule',
	'Compound': 'compound',
	'Reaction': 'reaction',
	'ReactionParticipant': 'reaction_participant',
	'Organism': 'organism',
	'Evidence': 'evidence',
	'ProteinProteinInteraction': 'protein_protein_interaction'
}


function qualifyERI(eri) {

	return prefix + type + '/' + eri

}

function crackQualifiedERI(eri) {

	const [ type, id ] = eri.slice(prefix.length).split('/')

	return { type, id }
}

function typeToTable(type) {

	const table = tables[type]

	if(table) {
		return table + ''
	}

	throw new Error('Unknown type: ' + type + '; valid types are: ' + Object.keys(tables).join(', '))
}

export async function getERIForURIs(uris, type) {

	const row = await db.oneOrNone('SELECT eri FROM ' + typeToTable(type) + ' WHERE uri IN ($1:csv) LIMIT 1', [ uris ])

	if(row) {
		return prefix + type + '/' + row.eri
	}

	return null
}


// TODO: race condition between finding that eri does not exist and creating it

export async function getOrCreateERIForURIs(uris, type) {

	var eri = await getERIForURIs(uris, type)

	if(eri === null) {

		return prefix + type + '/' + (await mintNewERI(uris, type))

	}

	return eri
}


export async function getURIsForERI(eri) {

	const { id, type } = crackQualifiedERI(eri)

	const rows = await db.manyOrNone('SELECT uri FROM ' + typeToTable(type) + ' WHERE eri = $1', [ id  ])

	return rows.map((row) => row.uri)
}

function generateID() {
        return crypto.randomBytes(6).toString('hex').toUpperCase()
}


export async function mintNewERI(uris, type) {

	const allUris = await findXRefs(uris, type)

	const eri = generateID()

	await storeMapping(eri, allUris, type)

	return eri
}

export async function findXRefs(uris, type) {

	// need to call getXRefs on all microservices

	const config = getConfig()

	if(config.services.length === 0) {
		return uris
	}

	var oldUris = new Set(uris)
	var newUris = new Set(uris)

	console.log('findXRefs: initial set: ' + JSON.stringify(uris))

	let res = await iterate()

    console.log('findXRefs: done')

    return res

	async function iterate() {

        var n = config.services.length

		return await new Promise((resolve, reject) => {

            console.log('findXRefs: ' + config.services.length + ' service(s)')

			for(let service of config.services) {

                console.log('requesting from ' + service.url)

				request({
					method: 'POST',
					url: service.url + '/getXRefs',
					json: true,
					body: {
						type: type,
						uris: Array.from(newUris)
					}
				}, (err, res, body) => {

                    console.log(service + ' responded')

                    console.log('from service', JSON.stringify(body))

					if(err) {

						console.log('findXRefs: ' + service.name + ' error')
						console.dir(err)

						reject(err)

						return
					}

					for(let uri of body.uris) {

						console.log('findXRefs: ' + service.name + ' added: ' + uri)

						newUris.add(uri)
					}

                    console.log('left n', n)

					if((-- n) === 0) {

						// all microservices have responded

                        console.log('all services have responded, new ' + newUris.size + ' old ' + oldUris.size)

						if(newUris.size === oldUris.size) {

							// nothing new was added; we are done

							resolve(Array.from(newUris))

						} else {
							
							// something new was added; iterate again

							oldUris = new Set(newUris)
							newUris = new Set(newUris)

							iterate().then(resolve).catch(reject)
						}
					}
				})
			}
		
		})
	}


}

export async function storeMapping(eri, uris, type) {

	const cs = new pgp.helpers.ColumnSet(['uri', 'eri'], { table: typeToTable(type) });

	const values = uris.map((uri) => {
		return {
			uri: uri,
			eri: eri
		}
	})

	const query = pgp.helpers.insert(values, cs);

	await db.none(query)

}



