type HighTightFlagBox = {
  startTime: string;
  endTime: string;
  lowerPrice: number;
  upperPrice: number;
};

type PrimitiveOptions = {
  box: HighTightFlagBox | null;
};

type PrimitiveAttachment = {
  chart: {
    timeScale(): {
      timeToCoordinate(time: string): number | null;
    };
  };
  series: {
    priceToCoordinate(price: number): number | null;
  };
  requestUpdate(): void;
};

class HighTightFlagPaneRenderer {
  constructor(private readonly source: HighTightFlagPrimitive) {}

  draw() {
    // Keep foreground rendering explicit and empty. We only paint the setup box
    // behind candles and moving averages.
  }

  drawBackground(target: any) {
    const attachment = this.source.attachment;
    const box = this.source.box;
    if (!attachment || !box) {
      return;
    }

    target.useMediaCoordinateSpace((scope: any) => {
      const ctx = scope.context;
      const x1 = attachment.chart.timeScale().timeToCoordinate(box.startTime);
      const x2 = attachment.chart.timeScale().timeToCoordinate(box.endTime);
      const y1 = attachment.series.priceToCoordinate(box.upperPrice);
      const y2 = attachment.series.priceToCoordinate(box.lowerPrice);
      if (x1 == null || x2 == null || y1 == null || y2 == null) {
        return;
      }

      const left = Math.min(x1, x2);
      const top = Math.min(y1, y2);
      const width = Math.max(Math.abs(x2 - x1), 3);
      const height = Math.max(Math.abs(y2 - y1), 3);

      ctx.save();
      try {
        ctx.beginPath();
        ctx.rect(left, top, width, height);
        ctx.fillStyle = "rgba(250, 204, 21, 0.10)";
        ctx.fill();
        ctx.strokeStyle = "rgba(250, 204, 21, 0.80)";
        ctx.lineWidth = 1;
        ctx.setLineDash([5, 4]);
        ctx.stroke();
      } finally {
        ctx.restore();
      }
    });
  }
}

class HighTightFlagPaneView {
  constructor(private readonly source: HighTightFlagPrimitive) {}

  zOrder() {
    return "bottom";
  }

  renderer() {
    return new HighTightFlagPaneRenderer(this.source);
  }
}

export class HighTightFlagPrimitive {
  private readonly paneView: HighTightFlagPaneView;
  attachment: PrimitiveAttachment | null = null;
  box: HighTightFlagBox | null;

  constructor(options: PrimitiveOptions) {
    this.box = options.box;
    this.paneView = new HighTightFlagPaneView(this);
  }

  attached(params: PrimitiveAttachment) {
    this.attachment = params;
    params.requestUpdate();
  }

  detached() {
    this.attachment = null;
  }

  paneViews() {
    return this.box ? [this.paneView] : [];
  }

  updateAllViews() {
    if (this.attachment) {
      this.attachment.requestUpdate();
    }
  }
}

export function createHighTightFlagPrimitive(box: HighTightFlagBox | null) {
  return new HighTightFlagPrimitive({ box });
}
