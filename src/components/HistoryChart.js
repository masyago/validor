import { useState } from "react";
import { flagPointColor, fmtChartDate, fmtAxisNum } from "../lib/format";

// Renders a single-analyte trend: value over time with dashed low/high
// reference lines. Kept as a top-level component (not nested in Home) so its
// hover state isn't torn down on every parent re-render.
function HistoryChart({ series, unit, refLow, refHigh }) {
  const [hoverIdx, setHoverIdx] = useState(null);
  if (!series || series.length === 0) return null;

  const width = 300;
  const height = 132;
  const padLeft = 34;
  const padRight = 46;
  const padTop = 14;
  const padBottom = 26;
  const plotW = width - padLeft - padRight;
  const plotH = height - padTop - padBottom;
  const plotBottom = padTop + plotH;

  const values = series.map((s) => s.value_num);
  const refVals = [refLow, refHigh].filter((v) => typeof v === "number");
  const allVals = values.concat(refVals);
  let yMin = Math.min(...allVals);
  let yMax = Math.max(...allVals);
  if (yMin === yMax) {
    yMin -= 1;
    yMax += 1;
  }
  // Extra headroom (vs. a plain 5-10%) so a value sitting below the low
  // reference line still has visible separation from the axis and doesn't
  // read as clipped/crowded against the ref line or the plot edge.
  const pad = (yMax - yMin) * 0.18;
  yMin -= pad;
  yMax += pad;

  const xAt = (i) =>
    series.length === 1 ? padLeft + plotW / 2 : padLeft + (i / (series.length - 1)) * plotW;
  const yAt = (v) => padTop + (1 - (v - yMin) / (yMax - yMin)) * plotH;

  const points = series.map((s, i) => ({ ...s, x: xAt(i), y: yAt(s.value_num) }));
  const linePath = points.map((p, i) => `${i === 0 ? "M" : "L"}${p.x.toFixed(1)},${p.y.toFixed(1)}`).join(" ");
  const hovered = hoverIdx != null ? points[hoverIdx] : null;

  // Three y-axis labels: the axis/x-axis intersection (bottom), a midpoint,
  // and one near the top (nudged down slightly so its text isn't clipped).
  const yTicks = [
    { value: yMin, y: plotBottom },
    { value: (yMin + yMax) / 2, y: (padTop + plotBottom) / 2 },
    { value: yMax, y: padTop + 3 },
  ];

  return (
    <div className="hist-chart">
      <svg width={width} height={height} className="hist-chart-svg">
        <line x1={padLeft} x2={padLeft} y1={padTop} y2={plotBottom} className="hist-chart-axis" />
        <line x1={padLeft} x2={width - padRight} y1={plotBottom} y2={plotBottom} className="hist-chart-axis" />
        {yTicks.map((t, i) => (
          <g key={i}>
            <line x1={padLeft - 4} x2={padLeft} y1={t.y} y2={t.y} className="hist-chart-axis" />
            <text x={padLeft - 7} y={t.y} textAnchor="end" dominantBaseline="middle" className="hist-chart-axis-label">
              {fmtAxisNum(t.value)}
            </text>
          </g>
        ))}
        {refLow != null && (
          <>
            <line x1={padLeft} x2={width - padRight} y1={yAt(refLow)} y2={yAt(refLow)} className="hist-chart-refline" />
            <text x={width - padRight + 4} y={yAt(refLow)} dominantBaseline="middle" className="hist-chart-refline-label">
              {refLow}
            </text>
          </>
        )}
        {refHigh != null && (
          <>
            <line x1={padLeft} x2={width - padRight} y1={yAt(refHigh)} y2={yAt(refHigh)} className="hist-chart-refline" />
            <text x={width - padRight + 4} y={yAt(refHigh)} dominantBaseline="middle" className="hist-chart-refline-label">
              {refHigh}
            </text>
          </>
        )}
        {points.length > 1 && <path d={linePath} className="hist-chart-line" fill="none" />}
        {points.map((p, i) => (
          <g key={i} onMouseEnter={() => setHoverIdx(i)} onMouseLeave={() => setHoverIdx(null)}>
            <circle cx={p.x} cy={p.y} r={10} className="hist-chart-hit" />
            <circle cx={p.x} cy={p.y} r={p.isCurrent ? 5 : 3.5} style={{ fill: flagPointColor(p.flag) }} />
          </g>
        ))}
        <text x={padLeft} y={height - 4} className="hist-chart-axis-label">
          {fmtChartDate(points[0].effective_at)}
        </text>
        {points.length > 1 && (
          <text x={width - padRight} y={height - 4} textAnchor="end" className="hist-chart-axis-label">
            {fmtChartDate(points[points.length - 1].effective_at)}
          </text>
        )}
      </svg>
      {hovered && (
        <div
          className="hist-chart-tooltip"
          style={{ left: `${(hovered.x / width) * 100}%`, top: `${(hovered.y / height) * 100}%` }}
        >
          <div className="hist-chart-tooltip-val">
            {hovered.value_num}
            {unit ? ` ${unit}` : ""}
          </div>
          <div className="hist-chart-tooltip-date">{fmtChartDate(hovered.effective_at)}</div>
          {hovered.flag && hovered.flag !== "unknown" && (
            <div className="hist-chart-tooltip-flag" style={{ color: flagPointColor(hovered.flag) }}>
              {hovered.flag.toUpperCase().replace("_", " ")}
            </div>
          )}
        </div>
      )}
    </div>
  );
}

export default HistoryChart;
