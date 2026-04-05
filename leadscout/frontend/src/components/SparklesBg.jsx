import React from "react";
import Particles, { initParticlesEngine } from "@tsparticles/react"
import { loadSlim } from "@tsparticles/slim"
import { motion, useAnimation } from "framer-motion"
import { useEffect, useId, useState } from "react"

function SparkLayer({ id, color, density, speed, minSize, maxSize }) {
  const [init, setInit] = useState(false)
  const controls = useAnimation()
  const genId = useId()

  useEffect(() => {
    initParticlesEngine(async (engine) => {
      await loadSlim(engine)
    }).then(() => setInit(true))
  }, [])

  return (
    <motion.div
      animate={controls}
      style={{ position: "absolute", inset: 0, opacity: 0 }}
    >
      {init && (
        <Particles
          id={id || genId}
          style={{ width: "100%", height: "100%" }}
          particlesLoaded={async (c) => { if (c) controls.start({ opacity: 1, transition: { duration: 1.5 } }) }}
          options={{
            background: { color: { value: "transparent" } },
            fullScreen: { enable: false },
            fpsLimit: 60,
            particles: {
              color: { value: color },
              move: {
                enable: true,
                speed: { min: 0.05, max: speed || 0.4 },
                direction: "none",
                outModes: { default: "out" },
              },
              number: {
                density: { enable: true, width: 800, height: 800 },
                value: density || 60,
              },
              opacity: {
                value: { min: 0.1, max: 0.8 },
                animation: { enable: true, speed: 0.8, sync: false, startValue: "random" },
              },
              size: {
                value: { min: minSize || 0.3, max: maxSize || 1.5 },
              },
              twinkle: {
                particles: { enable: true, frequency: 0.08, opacity: 1 },
              },
            },
            detectRetina: true,
          }}
        />
      )}
    </motion.div>
  )
}

export default function SparklesBg() {
  return (
    <div className="sparkles-bg">
      <SparkLayer id="sp-1" color="#ffffff" density={50}  speed={0.25} minSize={0.2} maxSize={1.0} />
      <SparkLayer id="sp-2" color="#ffffff" density={25}  speed={0.15} minSize={0.2} maxSize={0.7} />
      <SparkLayer id="sp-3" color="#ffffff" density={15}  speed={0.10} minSize={0.3} maxSize={0.8} />
    </div>
  )
}