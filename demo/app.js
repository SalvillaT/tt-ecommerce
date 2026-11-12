(() => {
  const goldButton = document.querySelector('#show-gold');
  const qualityButton = document.querySelector('#show-quality');
  const goldPanel = document.querySelector('#gold-view');
  const qualityPanel = document.querySelector('#quality-view');
  const status = document.querySelector('#view-status');

  if (!goldButton || !qualityButton || !goldPanel || !qualityPanel || !status) return;

  const selectView = (view) => {
    const isGold = view === 'gold';
    goldButton.setAttribute('aria-pressed', String(isGold));
    qualityButton.setAttribute('aria-pressed', String(!isGold));
    goldButton.classList.toggle('is-active', isGold);
    qualityButton.classList.toggle('is-active', !isGold);
    status.textContent = `Vista seleccionada: ${isGold ? 'Gold publicado' : 'Evidencia de calidad'}`;
    const panel = isGold ? goldPanel : qualityPanel;
    const reducedMotion = window.matchMedia('(prefers-reduced-motion: reduce)').matches;
    panel.scrollIntoView({ behavior: reducedMotion ? 'instant' : 'smooth', block: 'start' });
    panel.focus({ preventScroll: true });
  };

  goldButton.addEventListener('click', () => selectView('gold'));
  qualityButton.addEventListener('click', () => selectView('quality'));
})();
