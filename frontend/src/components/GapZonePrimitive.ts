type PrimitiveGapZone = {
  startTime: string;
  endTime: string;
  lowerPrice: number;
  upperPrice: number;
  direction: "up" | "down";
};

type PrimitiveOptions = {
  zones: PrimitiveGapZone[];
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

class GapZonePaneRenderer {
  constructor(
    private readonly source: GapZonePrimitive,
  ) {}

  draw() {
    // The primitive only paints background gap boxes. The foreground hook still
    // gets called by lightweight-charts, so keep this as an explicit no-op.
  }

  drawBackground(target: any) {
    const attachment = this.source.attachment;
    if (!attachment || this.source.zones.length === 0) {
      return;
    }

    target.useMediaCoordinateSpace((scope: any) => {
      const ctx = scope.context;
      ctx.save();
      try {
        for (const zone of this.source.zones) {
          const x1 = attachment.chart.timeScale().timeToCoordinate(zone.startTime);
          const x2 = attachment.chart.timeScale().timeToCoordinate(zone.endTime);
          const y1 = attachment.series.priceToCoordinate(zone.upperPrice);
          const y2 = attachment.series.priceToCoordinate(zone.lowerPrice);
          if (x1 == null || x2 == null || y1 == null || y2 == null) {
            continue;
          }

          const left = Math.min(x1, x2);
          const top = Math.min(y1, y2);
          const width = Math.max(Math.abs(x2 - x1), 2);
          const height = Math.max(Math.abs(y2 - y1), 2);
          const stroke = zone.direction === "up" ? "rgba(110, 231, 183, 0.8)" : "rgba(252, 165, 165, 0.8)";
          const fill = zone.direction === "up" ? "rgba(34, 197, 94, 0.12)" : "rgba(239, 68, 68, 0.12)";

          ctx.beginPath();
          ctx.rect(left, top, width, height);
          ctx.fillStyle = fill;
          ctx.fill();
          ctx.strokeStyle = stroke;
          ctx.lineWidth = 1;
          ctx.stroke();
        }
      } finally {
        ctx.restore();
      }
    });
  }
}

class GapZonePaneView {
  constructor(private readonly source: GapZonePrimitive) {}

  zOrder() {
    return "bottom";
  }

  renderer() {
    return new GapZonePaneRenderer(this.source);
  }
}

export class GapZonePrimitive {
  private readonly paneView: GapZonePaneView;
  attachment: PrimitiveAttachment | null = null;
  zones: PrimitiveGapZone[];

  constructor(options: PrimitiveOptions) {
    this.zones = options.zones;
    this.paneView = new GapZonePaneView(this);
  }

  attached(params: PrimitiveAttachment) {
    this.attachment = params;
    params.requestUpdate();
  }

  detached() {
    this.attachment = null;
  }

  paneViews() {
    return [this.paneView];
  }

  updateAllViews() {
    if (this.attachment) {
      this.attachment.requestUpdate();
    }
  }
}

export function createGapZonePrimitive(
  zones: Array<{
    startTime: string;
    endTime: string;
    lowerPrice: number;
    upperPrice: number;
    direction: "up" | "down";
  }>,
) {
  return new GapZonePrimitive({ zones });
}
