function App() {

  const statement = {
  statement_id: "stmt_001",
  account_holder: "Jane Doe",
  account_number: "12345678",
  sort_code: "40-00-01",
  transactions: [
    { id: "t1", date: "2026-09-01", raw_description: "EMPLOYER SALARY BGC", amount: "2800.00" },
    { id: "t2", date: "2026-09-03", raw_description: "RENT PAYMENT TO LANDLORD", amount: "-950.00" },
    { id: "t3", date: "2026-09-05", raw_description: "POS 4829 BET365 UK", amount: "-40.00" },
  ],
  };
  
  async function handleClick() {
    const response = await fetch('http://localhost:8000/api/v1/statements/ingest', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(statement),
    });
    if (!response.ok) throw new Error(`HTTP ${response.status}`);

    const { job_id } = await response.json();
    console.log('job_id:', job_id);

    const ws = new WebSocket(`ws://localhost:8000/api/v1/ws/underwriting/${job_id}`);
    ws.onmessage = (event) => console.log('WS message:', JSON.parse(event.data));
    ws.onerror = (err) => console.error('WS error:', err);
    ws.onclose = () => console.log('WS closed');
  }

  

  return (
    <>
      <section id="center">
        <div className="hero">
          
        <button
          type="button"
          onClick={handleClick}
        >
          Send POST Request
        </button>
        </div>
      </section>
 

    </>
  )
}

export default App
