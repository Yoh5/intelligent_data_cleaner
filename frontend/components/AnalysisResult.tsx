'use client';

import React, { useState, useCallback } from 'react';
import { IssueCard, Issue } from './IssueCard';

interface AnalysisResultProps {
  analysis: {
    shape: [number, number];
    columns: Record<string, any>;
    issues: Array<{
      column: string;
      issue: string;
      severity: 'high' | 'medium' | 'low' | 'critical';
      count?: number;
      rate?: number;
      semantic_type?: string;
      description?: string;
      affected_rows?: number;
      type?: string;
    }>;
    dataset_info?: {
      filename: string;
    };
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
        filename: analysis.dataset_info?.filename || 'dataset.csv',
        shape: analysis.shape,
        timestamp: new Date().toISOString()
      };

      // Filtrer les étapes sélectionnées
      const selectedIssues = analysis.issues.filter(issue =>
        selectedSteps.has(`${issue.column}-${issue.issue}`)
      );

      // Formater pour l'API
      const steps = selectedIssues.map(issue => ({
        column: issue.column,
        issue_type: issue.issue,
        strategy_name: issue.severity,
        code: ''  // Sera rempli par le backend
      }));

      // Appel API pour générer le code
      const { generateCode } = await import('@/lib/api');
      const response = await generateCode({
        dataset_name: metadata.filename,
        steps: steps
      });

      if (response.script) {
        setPreview(response.script);
        onGenerateCode(response.script);
      }
    } catch (error) {
      console.error('Erreur:', error);
      alert('Erreur lors de la génération du code');
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
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  };

  return (
    <div className="space-y-6 max-w-4xl mx-auto p-6">
      {/* Résumé */}
      <div className="grid grid-cols-3 gap-4">
        <div className="bg-white shadow rounded-lg p-4 text-center">
          <h3 className="text-sm font-medium text-gray-500">Dimensions</h3>
          <p className="text-2xl font-bold text-gray-900">
            {analysis.shape[0]} × {analysis.shape[1]}
          </p>
        </div>
        <div className="bg-white shadow rounded-lg p-4 text-center">
          <h3 className="text-sm font-medium text-gray-500">Problèmes détectés</h3>
          <p className="text-2xl font-bold text-gray-900">
            {analysis.issues.length}
          </p>
        </div>
        <div className="bg-white shadow rounded-lg p-4 text-center">
          <h3 className="text-sm font-medium text-gray-500">Critiques</h3>
          <p className="text-2xl font-bold text-red-600">
            {analysis.issues.filter(i => i.severity === 'critical' || i.severity === 'high').length}
          </p>
        </div>
      </div>

      {/* Liste des problèmes */}
      <div className="bg-white shadow rounded-lg p-6">
        <div className="flex justify-between items-center mb-4">
          <h3 className="text-lg font-semibold">Problèmes identifiés</h3>
          <button 
            onClick={selectAllCritical}
            className="text-sm text-blue-600 hover:text-blue-800 font-medium"
          >
            Sélectionner critiques
          </button>
        </div>

        <div className="space-y-3">
          {analysis.issues.map((issue, idx) => (
            <div key={idx} className="flex items-start gap-3">
              <input
                type="checkbox"
                checked={selectedSteps.has(`${issue.column}-${issue.issue}`)}
                onChange={() => toggleStep(`${issue.column}-${issue.issue}`)}
                className="mt-4 w-4 h-4 text-blue-600 rounded focus:ring-blue-500"
              />
              <div className="flex-1">
                <IssueCard
                  issue={{
                    type: issue.issue,
                    severity: issue.severity,
                    column: issue.column,
                    description: issue.description || `${issue.count} valeurs manquantes (${((issue.rate || 0) * 100).toFixed(1)}%)`,
                    affected_rows: issue.affected_rows || issue.count,
                    count: issue.count,
                    rate: issue.rate,
                    semantic_type: issue.semantic_type
                  }}
                  index={idx}
                  isSelected={selectedSteps.has(`${issue.column}-${issue.issue}`)}
                />
              </div>
            </div>
          ))}
        </div>
      </div>

      {/* Actions */}
      <div className="flex gap-4">
        <button
          onClick={handleGenerateCode}
          disabled={isGenerating || selectedSteps.size === 0}
          className="flex-1 bg-blue-600 text-white py-3 px-6 rounded-lg font-medium hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
        >
          {isGenerating ? 'Génération...' : `Générer le script (${selectedSteps.size} étapes)`}
        </button>
      </div>

      {/* Aperçu du code */}
      {preview && (
        <div className="bg-gray-900 rounded-lg overflow-hidden">
          <div className="flex justify-between items-center px-4 py-2 bg-gray-800">
            <h3 className="text-white font-medium">clean_dataset.py</h3>
            <button
              onClick={downloadCode}
              className="text-sm text-gray-300 hover:text-white"
            >
              Télécharger
            </button>
          </div>
          <pre className="p-4 overflow-x-auto">
            <code className="text-sm text-gray-100 font-mono whitespace-pre">
              {preview}
            </code>
          </pre>
        </div>
      )}
    </div>
  );
};

export default AnalysisResult;