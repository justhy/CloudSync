import * as React from "react"
import {
  AlertDialog,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from "@/components/ui/alert-dialog"
import { Button } from "@/components/ui/button"

interface ConfirmOptions {
  title: string
  description?: React.ReactNode
  confirmText?: string
  cancelText?: string
  destructive?: boolean
}

type ConfirmFn = (opts: ConfirmOptions) => Promise<boolean>

const ConfirmContext = React.createContext<ConfirmFn>(() => Promise.resolve(false))

/** 用 Promise 化的确认框替代 window.confirm，样式与主题保持一致。 */
export function useConfirm(): ConfirmFn {
  return React.useContext(ConfirmContext)
}

export function ConfirmProvider({ children }: { children: React.ReactNode }) {
  const [open, setOpen] = React.useState(false)
  const [opts, setOpts] = React.useState<ConfirmOptions>({ title: "" })
  // 关闭时（无论确认还是取消）都要把结果送回调用方，否则 Promise 永远悬着。
  const resolver = React.useRef<((v: boolean) => void) | null>(null)

  const confirm = React.useCallback<ConfirmFn>((options) => {
    setOpts(options)
    setOpen(true)
    return new Promise<boolean>((resolve) => {
      resolver.current = resolve
    })
  }, [])

  const finish = (value: boolean) => {
    setOpen(false)
    resolver.current?.(value)
    resolver.current = null
  }

  return (
    <ConfirmContext.Provider value={confirm}>
      {children}
      <AlertDialog
        open={open}
        onOpenChange={(next) => {
          if (!next) finish(false)
          else setOpen(true)
        }}
      >
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>{opts.title}</AlertDialogTitle>
            {opts.description ? (
              <AlertDialogDescription>{opts.description}</AlertDialogDescription>
            ) : null}
          </AlertDialogHeader>
          <AlertDialogFooter>
            {/* 不用 AlertDialogAction：Radix 会自己关弹窗再回调 onOpenChange，
                与 finish 抢顺序。这里两个按钮都显式结束 Promise。 */}
            <Button variant="outline" onClick={() => finish(false)}>
              {opts.cancelText ?? "取消"}
            </Button>
            <Button
              variant={opts.destructive === false ? "default" : "destructive"}
              onClick={() => finish(true)}
            >
              {opts.confirmText ?? "确认"}
            </Button>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </ConfirmContext.Provider>
  )
}
