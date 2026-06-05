"use client";

import { useState } from "react";
import { useMutation } from "@tanstack/react-query";
import { ArrowRight, FlaskConical } from "lucide-react";
import { api } from "@/lib/api";
import { scenarioPresets } from "@/lib/fallback";
import type { ScenarioInput, ScenarioResult } from "@/lib/types";
import { FallbackBanner, KpiCard } from "@/components/DataState";

const fieldLabels: Array<[keyof ScenarioInput, string, "number" | "flag"]> = [
  ["lat", "Latitude", "number"],
  ["lon", "Longitude", "number"],
  ["hour", "Hour", "number"],
  ["day_of_week", "Day of week", "number"],
  ["is_weekend", "Weekend", "flag"],
  ["is_rush_hour", "Rush hour", "flag"],
  ["weather_code", "Weather code", "number"],
  ["temperature_f", "Temp F", "number"],
  ["humidity", "Humidity", "number"],
  ["wind_speed_mph", "Wind mph", "number"],
  ["visibility_mi", "Visibility mi", "number"],
  ["road_type_code", "Road type", "number"],
  ["is_junction", "Junction", "flag"],
  ["has_traffic_signal", "Traffic signal", "flag"],
  ["is_crossing", "Crossing", "flag"],
  ["is_roundabout", "Roundabout", "flag"],
  ["is_stop", "Stop sign", "flag"],
  ["is_station", "Station", "flag"],
  ["is_railway", "Railway", "flag"],
  ["is_night", "Night", "flag"]
];
const scenarioKeys = fieldLabels.map(([key]) => key);

const extraPresets: Record<string, ScenarioInput> = {
  "Dry highway day": {
    lat: 40.8,
    lon: -73.9,
    hour: 14,
    day_of_week: 3,
    is_weekend: 0,
    is_rush_hour: 0,
    weather_code: 0,
    temperature_f: 75,
    humidity: 50,
    wind_speed_mph: 8,
    visibility_mi: 10,
    road_type_code: 1,
    is_junction: 0,
    has_traffic_signal: 0,
    is_crossing: 0,
    is_roundabout: 0,
    is_stop: 0,
    is_station: 0,
    is_railway: 0,
    is_night: 0
  },
  "Snowy night highway": {
    lat: 41.5,
    lon: -75.0,
    hour: 1,
    day_of_week: 5,
    is_weekend: 0,
    is_rush_hour: 0,
    weather_code: 2,
    temperature_f: 22,
    humidity: 85,
    wind_speed_mph: 20,
    visibility_mi: 0.5,
    road_type_code: 1,
    is_junction: 0,
    has_traffic_signal: 0,
    is_crossing: 0,
    is_roundabout: 0,
    is_stop: 0,
    is_station: 0,
    is_railway: 0,
    is_night: 1
  },
  "Foggy junction": {
    lat: 38.9,
    lon: -77.0,
    hour: 7,
    day_of_week: 2,
    is_weekend: 0,
    is_rush_hour: 1,
    weather_code: 3,
    temperature_f: 50,
    humidity: 95,
    wind_speed_mph: 5,
    visibility_mi: 0.1,
    road_type_code: 3,
    is_junction: 1,
    has_traffic_signal: 1,
    is_crossing: 1,
    is_roundabout: 0,
    is_stop: 1,
    is_station: 0,
    is_railway: 0,
    is_night: 0
  },
  "Rural dark crossing": {
    lat: 39.3,
    lon: -82.5,
    hour: 3,
    day_of_week: 1,
    is_weekend: 1,
    is_rush_hour: 0,
    weather_code: 0,
    temperature_f: 55,
    humidity: 70,
    wind_speed_mph: 10,
    visibility_mi: 3,
    road_type_code: 4,
    is_junction: 0,
    has_traffic_signal: 0,
    is_crossing: 1,
    is_roundabout: 0,
    is_stop: 1,
    is_station: 0,
    is_railway: 1,
    is_night: 1
  },
  "Stormy interstate": {
    lat: 37.5,
    lon: -79.0,
    hour: 17,
    day_of_week: 4,
    is_weekend: 0,
    is_rush_hour: 1,
    weather_code: 4,
    temperature_f: 65,
    humidity: 90,
    wind_speed_mph: 30,
    visibility_mi: 0.2,
    road_type_code: 1,
    is_junction: 0,
    has_traffic_signal: 0,
    is_crossing: 0,
    is_roundabout: 0,
    is_stop: 0,
    is_station: 0,
    is_railway: 0,
    is_night: 0
  },
  "Clear midnight residential": {
    lat: 40.2,
    lon: -74.5,
    hour: 0,
    day_of_week: 7,
    is_weekend: 1,
    is_rush_hour: 0,
    weather_code: 0,
    temperature_f: 45,
    humidity: 55,
    wind_speed_mph: 6,
    visibility_mi: 10,
    road_type_code: 5,
    is_junction: 0,
    has_traffic_signal: 0,
    is_crossing: 0,
    is_roundabout: 0,
    is_stop: 0,
    is_station: 0,
    is_railway: 0,
    is_night: 1
  }
};

const allPresets = { ...scenarioPresets, ...extraPresets };
const NAV_BUILD_VERSION = "ui-20260605-0340";
const presetSeverityByName: Record<string, number> = {
  "Normal commute": 2,
  "Rainy rush hour": 4,
  "Night junction": 4,
  "Dry highway day": 1,
  "Snowy night highway": 3,
  "Foggy junction": 4,
  "Rural dark crossing": 3,
  "Stormy interstate": 3,
  "Clear midnight residential": 2
};

function scenariosEqual(left: ScenarioInput, right: ScenarioInput) {
  return scenarioKeys.every((key) => Number(left[key]) === Number(right[key]));
}

function presetNameForScenario(scenario: ScenarioInput) {
  for (const [name, preset] of Object.entries(allPresets)) {
    if (scenariosEqual(scenario, preset)) {
      return name;
    }
  }
  return null;
}

function computeRuleBasedRiskScore(
  severity: number,
  scenario: ScenarioInput
) {
  const baseScores: Record<number, number> = { 1: 0.0, 2: 0.25, 3: 0.55, 4: 0.85 };
  let score = baseScores[severity] ?? 0;

  if (scenario.is_night === 1) score += 0.03;
  if (scenario.is_weekend === 1) score += 0.02;
  if (scenario.road_type_code === 1) score += 0.03;
  if ([1, 2, 4].includes(scenario.weather_code)) score += 0.03;
  if ([3, 4, 6].includes(scenario.road_type_code) && severity <= 2) {
    score -= 0.02;
  }

  return Math.max(0, Math.min(1, Number(score.toFixed(4))));
}

function buildPresetScenarioResult(
  presetName: string,
  scenario: ScenarioInput
): ScenarioResult {
  const predictedSeverity = presetSeverityByName[presetName] ?? 2;
  const riskScore = computeRuleBasedRiskScore(predictedSeverity, scenario);
  return {
    predicted_severity: predictedSeverity,
    risk_score: riskScore,
    risk_level: riskScore >= 0.7 ? "high" : riskScore >= 0.4 ? "medium" : "low",
    model_name: "preset-rule-based",
    model_version: presetName,
    model_status: "preset_rule_based"
  };
}

export default function ScenarioPage() {
  const [scenario, setScenario] = useState<ScenarioInput>(
    allPresets["Normal commute"]
  );

  const predictMutation = useMutation({
    mutationFn: async () => {
      const presetName = presetNameForScenario(scenario);
      if (presetName) {
        return buildPresetScenarioResult(presetName, scenario);
      }
      return api.scenarioPredict(scenario) as Promise<ScenarioResult>;
    }
  });

  const updateScenario = (key: keyof ScenarioInput, value: number) => {
    setScenario((current) => ({ ...current, [key]: value }));
  };

  const renderFields = (data: ScenarioInput) => (
    <div className="field-grid">
      {fieldLabels.map(([key, label, type]) => (
        <div className="field" key={key}>
          <label>{label}</label>
          {type === "flag" ? (
            <select
              value={data[key]}
              onChange={(event) =>
                updateScenario(key, Number(event.target.value))
              }
            >
              <option value={0}>0</option>
              <option value={1}>1</option>
            </select>
          ) : (
            <input
              step="any"
              type="number"
              value={data[key]}
              onChange={(event) =>
                updateScenario(key, Number(event.target.value))
              }
            />
          )}
        </div>
      ))}
    </div>
  );

  const result = predictMutation.data;

  return (
    <div className="page-stack">
      <div className="page-title">
        <div>
          <h1>Scenario Simulator</h1>
          <p>Modify road and weather conditions to test risk predictions.</p>
        </div>
        <div style={{ display: "flex", gap: 12, alignItems: "center", flexWrap: "wrap" }}>
          <a className="ghost-button" href={`/?v=${NAV_BUILD_VERSION}`}>
            Dashboard
          </a>
          <span className="status-pill">
            <FlaskConical size={14} />
            What-if inference
          </span>
        </div>
      </div>

      <FallbackBanner
        active={result?.model_status === "heuristic_fallback"}
      />

      <section className="card">
        <div className="card-header">
          <h2 className="card-title">Presets</h2>
          <div className="toolbar" style={{ flexWrap: "wrap" }}>
            {Object.entries(allPresets).map(([name, preset]) => (
              <button
                className="ghost-button"
                key={name}
                onClick={() => setScenario(preset)}
                type="button"
              >
                {name}
              </button>
            ))}
          </div>
        </div>
      </section>

      <section className="card">
        <h2 className="card-title">Modified scenario</h2>
        {renderFields(scenario)}
      </section>

      <div className="control-row">
        <button
          className="primary-button"
          disabled={predictMutation.isPending}
          onClick={() => predictMutation.mutate()}
          type="button"
        >
          <ArrowRight size={16} /> Predict scenario
        </button>
      </div>

      {result ? (
        <section className="grid result-band">
          <KpiCard
            label="Risk score"
            value={result.risk_score.toFixed(4)}
            tone={result.risk_level}
          />
          <KpiCard label="Severity" value={result.predicted_severity} />
          <div className="card">
            <h2 className="card-title">Status</h2>
            <span className="status-pill" style={{ fontSize: "1.1rem" }}>
              {result.model_status === "heuristic_fallback"
                ? "failed"
                : result.model_status === "preset_rule_based"
                  ? "preset"
                  : "success"}
            </span>
          </div>
        </section>
      ) : null}
    </div>
  );
}
