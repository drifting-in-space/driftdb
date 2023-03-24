import { DRIFTDB_URL } from '../config'
import React, { useState, useCallback, useEffect } from 'react'
import {
  StatusIndicator,
  useUniqueClientId,
  usePresence,
  useWebRtcPresence,
  DriftDBProvider
} from 'driftdb-react'

const HUE_OFFSET = (Math.random() * 360) | 0
const USER_COLOR = randomColor()
const RTC_COLOR = randomColor()

function SharedCanvas() {
  const username = useUniqueClientId()
  const userColor = USER_COLOR
  const [mousePosition, setMousePosition] = useState(null)
  const rtcMap = useWebRtcPresence({ name: username, mousePosition, RTC_COLOR }, 10)
  const presence = usePresence('users', { name: username, mousePosition, userColor })
  const drawing = React.useRef(false)

  const [ctx, setCtx] = useState(null)
  const setContext = useCallback((canvas) => {
    if (canvas === null) return
    const { width, height } = canvas.getBoundingClientRect()
    canvas.width = width
    canvas.height = height
    setCtx(canvas.getContext('2d'))
  }, [])

  React.useEffect(() => {
    if (ctx && !drawing.current) {
      drawing.current = true
      requestAnimationFrame(() => {
        drawCanvas(ctx, presence, rtcMap)
        drawing.current = false
      })
    }
  })

  const onMouseLeave = () => {
    setMousePosition(null)
  }

  const onMouseMove = (event) => {
    const boundingClientRects = event.currentTarget.getBoundingClientRect()
    const x = event.clientX - boundingClientRects.x
    const y = event.clientY - boundingClientRects.y
    setMousePosition([x, y])
  }

  return (
    <div>
      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-bold text-gray-800">DriftDB - Shared Canvas Demo</h1>
        <StatusIndicator />
      </div>
      <div className="mt-8">
        <canvas
          ref={setContext}
          className="w-full bg-gray-800 rounded-xl h-full"
          onMouseMove={onMouseMove}
          onMouseLeave={onMouseLeave}
        />
      </div>
    </div>
  )
}

function randomColor() {
  const h = (Math.random() * 40 + HUE_OFFSET) % 360 | 0
  const s = (Math.random() * 30 + 25) | 0
  const l = (Math.random() * 30 + 50) | 0
  return `hsla(${h}, ${s}%, ${l}%, 0.7)`
}

function drawCanvas(ctx, presence, webRtcPos) {
  if (!ctx) return
  ctx.clearRect(0, 0, ctx.canvas.width, ctx.canvas.height)

  const userArr = [...Object.values(webRtcPos), ...Object.values(presence)]
  userArr.map(({ value: { mousePosition, userColor, name } }) => {
    if (mousePosition) drawCursor(ctx, userColor, name, mousePosition)
  })
}

function drawCursor(ctx, userColor, name, mousePosition) {
  ctx.beginPath()
  ctx.fillStyle = userColor
  ctx.moveTo(mousePosition[0], mousePosition[1])
  ctx.arc(mousePosition[0], mousePosition[1], 5, 0, Math.PI * 2)
  ctx.fill()
  ctx.font = '12px monospace'
  ctx.fillText(name, mousePosition[0] + 8, mousePosition[1] - 8)
  ctx.strokeStyle = '#eee'
  ctx.lineWidth = 1
  ctx.strokeText(name, mousePosition[0] + 8, mousePosition[1] - 8)
}

export default function App() {
  return (
    <DriftDBProvider api={DRIFTDB_URL}>
      <SharedCanvas />
    </DriftDBProvider>
  )
}
