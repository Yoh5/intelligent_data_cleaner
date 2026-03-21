"use client";

import React, { useState, useCallback } from 'react';
import { Download, AlertCircle, CheckCircle, Code, Play } from 'lucide-react';

interface CleaningStep {
  type: string;
  column: string;
  description: string;
  code: string;
  priority: number;
  severity?: 'low' | 'medium' | 'high' | 'critical';
}

interface AnalysisResultProps {
  analysis: {
    shape: [number, number];
    columns: Record<string, {
      name: string;
      dtype: string;
      semantic_type?: string;
      missing_count: number;
      missing_rate: number;
    }>;
    issues: Array<{
      column: string;
      issue: string;
      severity: string;
      semantic_type?: string;
    }>;
  };
  originalData: Array<Record<string, any>>;
  onGenerateCode: (code: string) => void;
}

export const AnalysisResult: React.FC<AnalysisResultProps> = ({ 
  analysis, 
  originalData,
  onGenerateCode 
}) => {
  const [selectedSteps, setSelectedSteps] = useState<Set<string>>(new Set());
  const [isGenerating, setIsGenerating] = useState(false);
  const [preview, setPreview] = useState<string | null>(null);

  // Générer un échantillon réaliste pour le test
  const generateRealisticSample = useCallback(() => {
    if (!originalData || originalData.length === 0) return null;

    // Prendre max 100 lignes pour le test
    const sampleSize = Math.min(100, originalData.length);
    const sample = originalData.slice(0, sampleSize);

    // Convertir en format Python (dictionnaire)
    const columns = Object.keys(sample[0]);
    const dataDict: Record<string, any[]> = {};

    columns.forEach(col => {
      dataDict[col] = sample.map(row => {
        const val = row[col];
        // Gérer les valeurs spéciales pour Python
        if (val === '' || val === ' ' || val === null || val === undefined) return null;
        if (typeof val === 'string') {
          // Détecter si c'est une colonne numérique avec des strings
          const colProfile = analysis.columns[col];
          if (colProfile?.semantic_type?.includes('numeric')) {
            const num = parseFloat(val);
            return isNaN(num) ? null : num;
          }
        }
        return val;
      });
    });

    return { columns, data: dataDict, size: sampleSize };
  }, [originalData, analysis.columns]);

  const toggleStep = (stepId: string) => {
    const newSteps = new Set(selectedSteps);
    if (newSteps.has(stepId)) {
      newSteps.delete(stepId);
    } else {
      newSteps.add(stepId);
    }
    setSelectedSteps(newSteps);
  };

  const selectAllCritical = () => {
    const critical = analysis.issues
      .filter(issue => issue.severity === 'high' || issue.severity === 'critical')
      .map(issue => `${issue.column}-${issue.issue}`);
    setSelectedSteps(new Set(critical));
  };

  const handleGenerateCode = async () => {
    setIsGenerating(true);

    try {
      const sample = generateRealisticSample();

      // Préparer les métadonnées
      const metadata = {
        filename: 'dataset.csv',
        shape: analysis.shape,
        timestamp: new Date().toISOString()
      };

      // Filtrer les étapes sélectionnées
      const selectedIssues = analysis.issues.filter(issue => 
        selectedSteps.has(`${issue.column}-${issue.issue}`)
      );

      // Appel API pour générer le code
      const response = await fetch('/api/generate', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          issues: selectedIssues,
          columns: analysis.columns,
          metadata,
          sample_data: sample // Envoyer l'échantillon pour validation
        })
      });

      if (!response.ok) throw new Error('Erreur génération');

      const result = await response.json();

      if (result.code) {
        setPreview(result.code);
        onGenerateCode(result.code);
      }

      if (result.validation_result) {
        console.log('Validation:', result.validation_result);
      }
    } catch (error) {
      console.error('Erreur:', error);
    } finally {
      setIsGenerating(false);
    }
  };

  const downloadCode = () => {
    if (!preview) return;
    const blob = new Blob([preview], { type: 'text/plain' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = 'clean_dataset.py';
    a.click();
  };

  return (
    <div className="space-y-6">
      {/* Résumé */}
      <div className="grid grid-cols-3 gap-4">
        <div className="bg-blue-50 p-4 rounded-lg">
          <h3 className="text-blue-800 font-semibold">Dimensions</h3>
          <p className="text-2xl font-bold">{analysis.shape[0]} × {analysis.shape[1]}</p>
        </div>
        <div className="bg-yellow-50 p-4 rounded-lg">
          <h3 className="text-yellow-800 font-semibold">Problèmes détectés</h3>
          <p className="text-2xl font-bold">{analysis.issues.length}</p>
        </div>
        <div className="bg-red-50 p-4 rounded-lg">
          <h3 className="text-red-800 font-semibold">Critiques</h3>
          <p className="text-2xl font-bold">
            {analysis.issues.filter(i => i.severity === 'critical' || i.severity === 'high').length}
          </p>
        </div>
      </div>

      {/* Liste des problèmes */}
      <div className="bg-white border rounded-lg shadow-sm">
        <div className="p-4 border-b flex justify-between items-center">
          <h3 className="font-semibold text-lg">Problèmes identifiés</h3>
          <button 
            onClick={selectAllCritical}
            className="text-sm text-blue-600 hover:text-blue-800"
          >
            Sélectionner critiques
          </button>
        </div>

        <div className="divide-y">
          {analysis.issues.map((issue, idx) => (
            <div 
              key={idx}
              className={`p-4 flex items-start gap-3 hover:bg-gray-50 cursor-pointer ${
                selectedSteps.has(`${issue.column}-${issue.issue}`) ? 'bg-blue-50' : ''
              }`}
              onClick={() => toggleStep(`${issue.column}-${issue.issue}`)}
            >
              <input
                type="checkbox"
                checked={selectedSteps.has(`${issue.column}-${issue.issue}`)}
                onChange={() => {}}
                className="mt-1"
              />
              <div className="flex-1">
                <div className="flex items-center gap-2">
                  <span className="font-semibold">{issue.column}</span>
                  <span className={`text-xs px-2 py-1 rounded ${
                    issue.severity === 'critical' ? 'bg-red-100 text-red-800' :
                    issue.severity === 'high' ? 'bg-orange-100 text-orange-800' :
                    issue.severity === 'medium' ? 'bg-yellow-100 text-yellow-800' :
                    'bg-gray-100 text-gray-800'
                  }`}>
                    {issue.severity}
                  </span>
                  {issue.semantic_type && (
                    <span className="text-xs bg-purple-100 text-purple-800 px-2 py-1 rounded">
                      {issue.semantic_type}
                    </span>
                  )}
                </div>
                <p className="text-gray-600 text-sm mt-1">
                  {issue.issue === 'missing_values' && `${issue.count} valeurs manquantes (${(issue.rate * 100).toFixed(1)}%)`}
                  {issue.issue === 'mixed_types' && `Types mixtes détectés (suggéré: ${issue.suggested_dtype})`}
                  {issue.issue === 'duplicate_ids' && `${issue.duplicates} doublons détectés`}
                </p>
              </div>
              {issue.severity === 'high' && <AlertCircle className="text-orange-500" size={20} />}
            </div>
          ))}
        </div>
      </div>

      {/* Actions */}
      <div className="flex gap-4">
        <button
          onClick={handleGenerateCode}
          disabled={selectedSteps.size === 0 || isGenerating}
          className="flex-1 bg-blue-600 text-white py-3 rounded-lg font-semibold disabled:opacity-50 flex items-center justify-center gap-2"
        >
          {isGenerating ? (
            <>Validation en cours...</>
          ) : (
            <><Code size={20} /> Générer le script Python ({selectedSteps.size} étapes)</>
          )}
        </button>
      </div>

      {/* Aperçu du code */}
      {preview && (
        <div className="bg-gray-900 rounded-lg overflow-hidden">
          <div className="bg-gray-800 px-4 py-2 flex justify-between items-center">
            <span className="text-gray-200 font-mono text-sm">clean_dataset.py</span>
            <button
              onClick={downloadCode}
              className="text-gray-300 hover:text-white flex items-center gap-2 text-sm"
            >
              <Download size={16} /> Télécharger
            </button>
          </div>
          <pre className="p-4 text-green-400 font-mono text-sm overflow-x-auto max-h-96">
            {preview}
          </pre>
        </div>
      )}
    </div>
  );
};

export default AnalysisResult;