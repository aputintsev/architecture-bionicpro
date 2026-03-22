import React, { useState } from 'react';
import { useKeycloak } from '@react-keycloak/web';

interface ReportRow {
  customer_id: number;
  prosthetic_id: string;
  report_date: string;
  customer_name: string;
  total_events: number;
  avg_response_time_ms: number;
  min_response_time_ms: number;
  max_response_time_ms: number;
  avg_battery_level: number;
  avg_signal_quality: number;
  anomaly_count: number;
  most_common_movement: string;
}

interface ReportData {
  user: string;
  email: string;
  message?: string;
  rows: ReportRow[];
}

const ReportPage: React.FC = () => {
  const { keycloak, initialized } = useKeycloak();
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [report, setReport] = useState<ReportData | null>(null);

  const downloadReport = async () => {
    if (!keycloak?.token) {
      setError('Not authenticated');
      return;
    }

    try {
      setLoading(true);
      setError(null);
      setReport(null);

      const response = await fetch(`${process.env.REACT_APP_API_URL}/reports`, {
        headers: {
          'Authorization': `Bearer ${keycloak.token}`
        }
      });

      if (response.status === 401) {
        setError('Authentication failed. Please log in again.');
        return;
      }

      if (!response.ok) {
        const body = await response.json().catch(() => ({}));
        setError(body.detail || `Server error: ${response.status}`);
        return;
      }

      const data: ReportData = await response.json();
      setReport(data);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'An error occurred');
    } finally {
      setLoading(false);
    }
  };

  if (!initialized) {
    return <div>Loading...</div>;
  }

  if (!keycloak.authenticated) {
    return (
      <div className="flex flex-col items-center justify-center min-h-screen bg-gray-100">
        <button
          onClick={() => keycloak.login()}
          className="px-4 py-2 bg-blue-500 text-white rounded hover:bg-blue-600"
        >
          Login
        </button>
      </div>
    );
  }

  return (
    <div className="flex flex-col items-center justify-center min-h-screen bg-gray-100 p-4">
      <div className="w-full max-w-4xl p-8 bg-white rounded-lg shadow-md">
        <h1 className="text-2xl font-bold mb-2">Usage Reports</h1>
        <p className="text-gray-500 mb-6">Logged in as: {keycloak.tokenParsed?.preferred_username}</p>

        <button
          onClick={downloadReport}
          disabled={loading}
          className={`px-4 py-2 bg-blue-500 text-white rounded hover:bg-blue-600 ${
            loading ? 'opacity-50 cursor-not-allowed' : ''
          }`}
        >
          {loading ? 'Loading Report...' : 'Get My Report'}
        </button>

        {error && (
          <div className="mt-4 p-4 bg-red-100 text-red-700 rounded">
            {error}
          </div>
        )}

        {report && (
          <div className="mt-6">
            {report.message ? (
              <div className="p-4 bg-yellow-50 text-yellow-800 rounded">
                {report.message}
              </div>
            ) : (
              <>
                <h2 className="text-lg font-semibold mb-3">
                  Report for {report.user} ({report.email})
                </h2>
                <div className="overflow-x-auto">
                  <table className="w-full text-sm border-collapse border border-gray-200">
                    <thead className="bg-gray-50">
                      <tr>
                        <th className="border border-gray-200 px-3 py-2 text-left">Date</th>
                        <th className="border border-gray-200 px-3 py-2 text-left">Prosthetic</th>
                        <th className="border border-gray-200 px-3 py-2 text-right">Events</th>
                        <th className="border border-gray-200 px-3 py-2 text-right">Avg Response (ms)</th>
                        <th className="border border-gray-200 px-3 py-2 text-right">Avg Battery %</th>
                        <th className="border border-gray-200 px-3 py-2 text-right">Avg Signal %</th>
                        <th className="border border-gray-200 px-3 py-2 text-right">Anomalies</th>
                        <th className="border border-gray-200 px-3 py-2 text-left">Top Movement</th>
                      </tr>
                    </thead>
                    <tbody>
                      {report.rows.map((row, i) => (
                        <tr key={i} className={i % 2 === 0 ? 'bg-white' : 'bg-gray-50'}>
                          <td className="border border-gray-200 px-3 py-2">{row.report_date}</td>
                          <td className="border border-gray-200 px-3 py-2">{row.prosthetic_id}</td>
                          <td className="border border-gray-200 px-3 py-2 text-right">{row.total_events}</td>
                          <td className="border border-gray-200 px-3 py-2 text-right">{row.avg_response_time_ms.toFixed(1)}</td>
                          <td className="border border-gray-200 px-3 py-2 text-right">{row.avg_battery_level.toFixed(1)}</td>
                          <td className="border border-gray-200 px-3 py-2 text-right">{row.avg_signal_quality.toFixed(1)}</td>
                          <td className={`border border-gray-200 px-3 py-2 text-right ${row.anomaly_count > 0 ? 'text-red-600 font-semibold' : ''}`}>
                            {row.anomaly_count}
                          </td>
                          <td className="border border-gray-200 px-3 py-2">{row.most_common_movement}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </>
            )}
          </div>
        )}
      </div>
    </div>
  );
};

export default ReportPage;
