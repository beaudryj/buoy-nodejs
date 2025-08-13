import * as http from 'http'
import path from 'path'
import fs from 'fs'

export class HttpError extends Error {
    constructor(message: string, readonly statusCode: number) {
        super(message)
    }
}

export function getUUID(request: http.IncomingMessage) {
    const url = new URL(request.url || '', 'http://localhost')
    const uuid = url.pathname.slice(1)
    if (uuid.length < 10) {
        throw new HttpError('Invalid channel name', 400)
    }
    return uuid
}

export function readBody(request: http.IncomingMessage) {
    return new Promise<Buffer>((resolve, reject) => {
        const chunks: Buffer[] = []
        request.on('error', reject)
        request.on('data', (chunk) => {
            chunks.push(chunk)
        })
        request.on('end', () => {
            resolve(Buffer.concat(chunks))
        })
    })
}

export function readFileContent(filePath: string, rootPath?: string): string {
    if (rootPath) {
        filePath = path.join(rootPath, filePath)
    }
    if (filePath) {
        try {
            return fs.readFileSync(filePath, 'utf-8')
        } catch (error) {
            // eslint-disable-next-line no-console
            console.error('Failed to read file', filePath, error)
        }
    }
    return ''
}
