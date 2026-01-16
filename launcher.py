#!/usr/bin/env python3
"""
医美客户回访系统 - 图形化启动器

提供一个简单的图形界面来运行系统的各个功能，无需命令行操作。
"""
import sys
import asyncio
import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext
import threading
from pathlib import Path
from datetime import date
import logging
from queue import Queue

# 添加项目根目录
PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))


class TextHandler(logging.Handler):
    """将日志输出到文本框的处理器"""

    def __init__(self, text_widget, queue):
        super().__init__()
        self.text_widget = text_widget
        self.queue = queue

    def emit(self, record):
        msg = self.format(record)
        self.queue.put(msg + '\n')


class RevisitLauncher:
    """医美客户回访系统启动器"""

    def __init__(self):
        self.root = tk.Tk()
        self.root.title("医美客户回访系统 - 控制台")
        self.root.geometry("900x700")
        self.root.resizable(True, True)

        # 消息队列（用于线程间通信）
        self.log_queue = Queue()

        # 运行状态
        self.is_running = False
        self.current_task = None

        self._setup_ui()
        self._setup_logging()
        self._check_log_queue()

    def _setup_ui(self):
        """设置用户界面"""
        # 主框架
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.pack(fill=tk.BOTH, expand=True)

        # 标题
        title_label = ttk.Label(
            main_frame,
            text="🏥 医美客户回访系统",
            font=("Microsoft YaHei", 18, "bold")
        )
        title_label.pack(pady=(0, 10))

        # 状态栏
        self.status_var = tk.StringVar(value="就绪")
        status_frame = ttk.Frame(main_frame)
        status_frame.pack(fill=tk.X, pady=(0, 10))
        ttk.Label(status_frame, text="状态: ").pack(side=tk.LEFT)
        ttk.Label(status_frame, textvariable=self.status_var, foreground="green").pack(side=tk.LEFT)

        # Notebook（选项卡）
        notebook = ttk.Notebook(main_frame)
        notebook.pack(fill=tk.BOTH, expand=True)

        # ===== 第1个选项卡：快速操作 =====
        quick_frame = ttk.Frame(notebook, padding="10")
        notebook.add(quick_frame, text="🚀 快速操作")
        self._setup_quick_tab(quick_frame)

        # ===== 第2个选项卡：数据库管理 =====
        db_frame = ttk.Frame(notebook, padding="10")
        notebook.add(db_frame, text="🗄️ 数据库管理")
        self._setup_database_tab(db_frame)

        # ===== 第3个选项卡：数据导入 =====
        import_frame = ttk.Frame(notebook, padding="10")
        notebook.add(import_frame, text="📥 数据导入")
        self._setup_import_tab(import_frame)

        # ===== 第4个选项卡：生日回访 =====
        reminder_frame = ttk.Frame(notebook, padding="10")
        notebook.add(reminder_frame, text="🎂 生日回访")
        self._setup_reminder_tab(reminder_frame)

        # ===== 第5个选项卡：API服务 =====
        api_frame = ttk.Frame(notebook, padding="10")
        notebook.add(api_frame, text="🌐 API服务")
        self._setup_api_tab(api_frame)

        # ===== 日志输出区域 =====
        log_frame = ttk.LabelFrame(main_frame, text="📋 运行日志", padding="5")
        log_frame.pack(fill=tk.BOTH, expand=True, pady=(10, 0))

        self.log_text = scrolledtext.ScrolledText(
            log_frame,
            height=12,
            font=("Consolas", 9),
            state=tk.DISABLED
        )
        self.log_text.pack(fill=tk.BOTH, expand=True)

        # 日志控制按钮
        log_btn_frame = ttk.Frame(log_frame)
        log_btn_frame.pack(fill=tk.X, pady=(5, 0))
        ttk.Button(log_btn_frame, text="清空日志", command=self._clear_log).pack(side=tk.RIGHT)

    def _setup_quick_tab(self, parent):
        """设置快速操作选项卡"""
        # 说明
        desc = ttk.Label(
            parent,
            text="常用操作快捷入口，一键执行系统功能",
            font=("Microsoft YaHei", 10)
        )
        desc.pack(pady=(0, 15))

        # 按钮区域
        btn_frame = ttk.Frame(parent)
        btn_frame.pack(fill=tk.X)

        # 第一行按钮
        row1 = ttk.Frame(btn_frame)
        row1.pack(fill=tk.X, pady=5)

        ttk.Button(
            row1, text="🔍 系统检查", width=20,
            command=lambda: self._run_task(self._check_system)
        ).pack(side=tk.LEFT, padx=5)

        ttk.Button(
            row1, text="🔄 初始化数据库", width=20,
            command=lambda: self._run_task(self._init_database)
        ).pack(side=tk.LEFT, padx=5)

        ttk.Button(
            row1, text="📊 数据库状态", width=20,
            command=lambda: self._run_task(self._check_db_status)
        ).pack(side=tk.LEFT, padx=5)

        # 第二行按钮
        row2 = ttk.Frame(btn_frame)
        row2.pack(fill=tk.X, pady=5)

        ttk.Button(
            row2, text="📥 导入初始数据", width=20,
            command=lambda: self._run_task(self._import_initial_data)
        ).pack(side=tk.LEFT, padx=5)

        ttk.Button(
            row2, text="🎂 运行生日回访", width=20,
            command=lambda: self._run_task(self._run_reminders)
        ).pack(side=tk.LEFT, padx=5)

        ttk.Button(
            row2, text="🌐 启动API服务", width=20,
            command=self._start_api_service
        ).pack(side=tk.LEFT, padx=5)

        # 流程说明
        flow_frame = ttk.LabelFrame(parent, text="📌 运行流程", padding="10")
        flow_frame.pack(fill=tk.X, pady=(20, 0))

        flow_text = """
        完整的系统运行流程：

        1️⃣  系统检查 - 检查配置和数据库连接状态
        2️⃣  初始化数据库 - 创建数据库表结构（首次运行必须）
        3️⃣  导入初始数据 - 从JSON文件导入机构、客户、消费等数据
        4️⃣  启动API服务 - 启动Web API服务（可选）
        5️⃣  运行生日回访 - 查找即将生日的客户并生成回访内容

        💡 首次使用请按顺序执行1-3步，之后可直接执行第5步运行回访任务
        """

        ttk.Label(flow_frame, text=flow_text, justify=tk.LEFT).pack(anchor=tk.W)

    def _setup_database_tab(self, parent):
        """设置数据库管理选项卡"""
        # 数据库选项
        options_frame = ttk.LabelFrame(parent, text="初始化选项", padding="10")
        options_frame.pack(fill=tk.X, pady=(0, 10))

        self.skip_postgres = tk.BooleanVar()
        self.skip_nebula = tk.BooleanVar()
        self.skip_clickhouse = tk.BooleanVar()
        self.skip_qdrant = tk.BooleanVar()
        self.with_sample_data = tk.BooleanVar()
        self.force_reinit = tk.BooleanVar()

        row1 = ttk.Frame(options_frame)
        row1.pack(fill=tk.X, pady=2)
        ttk.Checkbutton(row1, text="跳过 PostgreSQL", variable=self.skip_postgres).pack(side=tk.LEFT, padx=10)
        ttk.Checkbutton(row1, text="跳过 NebulaGraph", variable=self.skip_nebula).pack(side=tk.LEFT, padx=10)

        row2 = ttk.Frame(options_frame)
        row2.pack(fill=tk.X, pady=2)
        ttk.Checkbutton(row2, text="跳过 ClickHouse", variable=self.skip_clickhouse).pack(side=tk.LEFT, padx=10)
        ttk.Checkbutton(row2, text="跳过 Qdrant", variable=self.skip_qdrant).pack(side=tk.LEFT, padx=10)

        row3 = ttk.Frame(options_frame)
        row3.pack(fill=tk.X, pady=2)
        ttk.Checkbutton(row3, text="插入示例数据（仅开发）", variable=self.with_sample_data).pack(side=tk.LEFT, padx=10)
        ttk.Checkbutton(row3, text="⚠️ 强制重建（删除现有数据）", variable=self.force_reinit).pack(side=tk.LEFT, padx=10)

        # 按钮
        btn_frame = ttk.Frame(parent)
        btn_frame.pack(fill=tk.X, pady=10)

        ttk.Button(
            btn_frame, text="初始化数据库", width=20,
            command=lambda: self._run_task(self._init_database_with_options)
        ).pack(side=tk.LEFT, padx=5)

        ttk.Button(
            btn_frame, text="仅检查连接", width=20,
            command=lambda: self._run_task(self._check_db_connections)
        ).pack(side=tk.LEFT, padx=5)

        # 数据库状态显示
        status_frame = ttk.LabelFrame(parent, text="数据库状态", padding="10")
        status_frame.pack(fill=tk.BOTH, expand=True, pady=(10, 0))

        self.db_status_text = scrolledtext.ScrolledText(
            status_frame,
            height=8,
            font=("Consolas", 9),
            state=tk.DISABLED
        )
        self.db_status_text.pack(fill=tk.BOTH, expand=True)

    def _setup_import_tab(self, parent):
        """设置数据导入选项卡"""
        # 说明
        ttk.Label(
            parent,
            text="将JSON格式的数据导入到PostgreSQL，并同步到其他数据库",
            font=("Microsoft YaHei", 10)
        ).pack(pady=(0, 15))

        # 导入类型
        type_frame = ttk.LabelFrame(parent, text="导入类型", padding="10")
        type_frame.pack(fill=tk.X, pady=(0, 10))

        self.import_type = tk.StringVar(value="initial")

        ttk.Radiobutton(
            type_frame, text="初始全量导入",
            variable=self.import_type, value="initial"
        ).pack(anchor=tk.W)

        ttk.Radiobutton(
            type_frame, text="增量数据导入",
            variable=self.import_type, value="incremental"
        ).pack(anchor=tk.W)

        # 按钮
        btn_frame = ttk.Frame(parent)
        btn_frame.pack(fill=tk.X, pady=10)

        ttk.Button(
            btn_frame, text="开始导入", width=20,
            command=lambda: self._run_task(self._import_data)
        ).pack(side=tk.LEFT, padx=5)

        ttk.Button(
            btn_frame, text="打开数据目录", width=20,
            command=self._open_data_dir
        ).pack(side=tk.LEFT, padx=5)

        # 数据目录说明
        path_frame = ttk.LabelFrame(parent, text="数据文件位置", padding="10")
        path_frame.pack(fill=tk.X, pady=(10, 0))

        paths_text = f"""
        初始数据目录: data/import/initial/
          - common/           公共数据（机构、医生、项目、产品）
          - institutions/     各机构业务数据（客户、消费记录）

        增量数据目录: data/import/incremental/
          - pending/          待处理的增量文件
          - processed/        已处理的增量文件

        📌 将JSON数据文件放入对应目录，然后执行导入
        """
        ttk.Label(path_frame, text=paths_text, justify=tk.LEFT).pack(anchor=tk.W)

    def _setup_reminder_tab(self, parent):
        """设置生日回访选项卡"""
        # 机构选择
        inst_frame = ttk.LabelFrame(parent, text="机构选择", padding="10")
        inst_frame.pack(fill=tk.X, pady=(0, 10))

        self.selected_institution = tk.StringVar(value="all")

        ttk.Radiobutton(
            inst_frame, text="所有机构",
            variable=self.selected_institution, value="all"
        ).pack(anchor=tk.W)

        # 从配置获取机构列表
        try:
            from config.settings import settings
            for inst in settings.APP.INSTITUTIONS:
                ttk.Radiobutton(
                    inst_frame, text=f"机构: {inst}",
                    variable=self.selected_institution, value=inst
                ).pack(anchor=tk.W)
        except Exception:
            pass

        # 选项
        opt_frame = ttk.LabelFrame(parent, text="运行选项", padding="10")
        opt_frame.pack(fill=tk.X, pady=(0, 10))

        self.test_mode = tk.BooleanVar(value=True)
        ttk.Checkbutton(
            opt_frame, text="测试模式（不实际发送消息）",
            variable=self.test_mode
        ).pack(anchor=tk.W)

        self.report_only = tk.BooleanVar()
        ttk.Checkbutton(
            opt_frame, text="仅查看报告（不生成回访内容）",
            variable=self.report_only
        ).pack(anchor=tk.W)

        # 按钮
        btn_frame = ttk.Frame(parent)
        btn_frame.pack(fill=tk.X, pady=10)

        ttk.Button(
            btn_frame, text="运行生日回访", width=20,
            command=lambda: self._run_task(self._run_reminder_task)
        ).pack(side=tk.LEFT, padx=5)

        ttk.Button(
            btn_frame, text="查看今日生日", width=20,
            command=lambda: self._run_task(self._show_today_birthdays)
        ).pack(side=tk.LEFT, padx=5)

        # 提醒配置显示
        try:
            from config.settings import settings
            config_text = f"""
            当前配置:
              提前天数: {settings.APP.BIRTHDAY_REMINDER_DAYS_AHEAD} 天
              机构列表: {', '.join(settings.APP.INSTITUTIONS)}
              今日日期: {date.today().isoformat()}
            """
            config_frame = ttk.LabelFrame(parent, text="配置信息", padding="10")
            config_frame.pack(fill=tk.X, pady=(10, 0))
            ttk.Label(config_frame, text=config_text, justify=tk.LEFT).pack(anchor=tk.W)
        except Exception:
            pass

    def _setup_api_tab(self, parent):
        """设置API服务选项卡"""
        # 说明
        ttk.Label(
            parent,
            text="启动Web API服务，提供RESTful接口",
            font=("Microsoft YaHei", 10)
        ).pack(pady=(0, 15))

        # API配置
        config_frame = ttk.LabelFrame(parent, text="API配置", padding="10")
        config_frame.pack(fill=tk.X, pady=(0, 10))

        try:
            from config.settings import settings
            config_text = f"""
            主机: {settings.API.HOST}
            端口: {settings.API.PORT}
            调试模式: {settings.API.DEBUG}
            
            API文档: http://{settings.API.HOST}:{settings.API.PORT}/api/docs
            """
            ttk.Label(config_frame, text=config_text, justify=tk.LEFT).pack(anchor=tk.W)
        except Exception as e:
            ttk.Label(config_frame, text=f"无法加载配置: {e}").pack()

        # 按钮
        btn_frame = ttk.Frame(parent)
        btn_frame.pack(fill=tk.X, pady=10)

        self.api_btn = ttk.Button(
            btn_frame, text="启动API服务", width=20,
            command=self._start_api_service
        )
        self.api_btn.pack(side=tk.LEFT, padx=5)

        ttk.Button(
            btn_frame, text="打开API文档", width=20,
            command=self._open_api_docs
        ).pack(side=tk.LEFT, padx=5)

        # API端点列表
        endpoints_frame = ttk.LabelFrame(parent, text="可用API端点", padding="10")
        endpoints_frame.pack(fill=tk.BOTH, expand=True, pady=(10, 0))

        endpoints_text = """
        客户管理:
          GET  /api/v1/customers/{institution_code}      - 获取机构客户列表
          GET  /api/v1/customers/{code}/profile          - 获取客户详情
          GET  /api/v1/customers/{code}/history          - 获取消费历史

        回访管理:
          GET  /api/v1/reminders/{institution_code}/upcoming-birthdays  - 即将生日客户
          GET  /api/v1/reminders/{institution_code}/today-birthdays     - 今日生日客户
          POST /api/v1/reminders/{institution_code}/run                 - 运行回访任务
          POST /api/v1/reminders/generate-content                       - 生成回访内容

        数据分析:
          GET  /api/v1/analytics/dashboard               - 仪表板数据
          GET  /api/v1/analytics/consumption-stats       - 消费统计
        """

        text_widget = scrolledtext.ScrolledText(
            endpoints_frame,
            height=10,
            font=("Consolas", 9)
        )
        text_widget.pack(fill=tk.BOTH, expand=True)
        text_widget.insert(tk.END, endpoints_text)
        text_widget.config(state=tk.DISABLED)

    def _setup_logging(self):
        """设置日志"""
        # 创建日志处理器
        handler = TextHandler(self.log_text, self.log_queue)
        handler.setFormatter(logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%H:%M:%S'
        ))

        # 添加到根日志记录器
        root_logger = logging.getLogger()
        root_logger.addHandler(handler)
        root_logger.setLevel(logging.INFO)

    def _check_log_queue(self):
        """检查日志队列并更新文本框"""
        while not self.log_queue.empty():
            msg = self.log_queue.get()
            self.log_text.config(state=tk.NORMAL)
            self.log_text.insert(tk.END, msg)
            self.log_text.see(tk.END)
            self.log_text.config(state=tk.DISABLED)

        self.root.after(100, self._check_log_queue)

    def _log(self, message: str):
        """添加日志消息"""
        self.log_queue.put(f"{message}\n")

    def _clear_log(self):
        """清空日志"""
        self.log_text.config(state=tk.NORMAL)
        self.log_text.delete(1.0, tk.END)
        self.log_text.config(state=tk.DISABLED)

    def _run_task(self, task_func):
        """在后台线程运行任务"""
        if self.is_running:
            messagebox.showwarning("提示", "有任务正在运行，请等待完成")
            return

        self.is_running = True
        self.status_var.set("运行中...")

        def run():
            try:
                # 创建新的事件循环
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    loop.run_until_complete(task_func())
                finally:
                    loop.close()
            except Exception as e:
                self._log(f"❌ 错误: {e}")
                logging.exception("任务执行失败")
            finally:
                self.is_running = False
                self.root.after(0, lambda: self.status_var.set("就绪"))

        thread = threading.Thread(target=run, daemon=True)
        thread.start()

    # ==================== 任务实现 ====================

    async def _check_system(self):
        """系统检查"""
        self._log("=" * 50)
        self._log("开始系统检查...")
        self._log("=" * 50)

        from scripts.check_system import (
            check_postgresql, check_nebulagraph,
            check_qdrant, check_clickhouse
        )
        from config.settings import settings

        # 显示配置
        self._log(f"\n配置信息:")
        self._log(f"  环境: {settings.APP.ENVIRONMENT}")
        self._log(f"  机构: {settings.APP.INSTITUTIONS}")

        # 检查各数据库
        self._log(f"\n数据库连接检查:")

        ok, msg = await check_postgresql()
        self._log(f"  PostgreSQL: {'✅' if ok else '❌'} {msg}")

        ok, msg = await check_nebulagraph()
        self._log(f"  NebulaGraph: {'✅' if ok else '❌'} {msg}")

        ok, msg = await check_clickhouse()
        self._log(f"  ClickHouse: {'✅' if ok else '❌'} {msg}")

        ok, msg = await check_qdrant()
        self._log(f"  Qdrant: {'✅' if ok else '❌'} {msg}")

        self._log("\n系统检查完成")

    async def _init_database(self):
        """初始化数据库（默认选项）"""
        self._log("=" * 50)
        self._log("开始初始化数据库...")
        self._log("=" * 50)

        from scripts.init_database import DatabaseInitializer
        import argparse

        args = argparse.Namespace(
            check_only=False,
            with_sample_data=False,
            skip_postgres=False,
            skip_nebula=False,
            skip_clickhouse=False,
            skip_qdrant=False,
            force=False,
            wait_docker=3
        )

        initializer = DatabaseInitializer(args)
        success = await initializer.initialize_all()

        if success:
            self._log("\n✅ 数据库初始化成功!")
        else:
            self._log("\n⚠️ 数据库初始化完成，但部分数据库可能有问题")

    async def _init_database_with_options(self):
        """使用选项初始化数据库"""
        self._log("=" * 50)
        self._log("开始初始化数据库（自定义选项）...")
        self._log("=" * 50)

        from scripts.init_database import DatabaseInitializer
        import argparse

        args = argparse.Namespace(
            check_only=False,
            with_sample_data=self.with_sample_data.get(),
            skip_postgres=self.skip_postgres.get(),
            skip_nebula=self.skip_nebula.get(),
            skip_clickhouse=self.skip_clickhouse.get(),
            skip_qdrant=self.skip_qdrant.get(),
            force=self.force_reinit.get(),
            wait_docker=3
        )

        if args.force:
            self._log("⚠️ 警告: 强制重建模式，将删除现有数据!")

        initializer = DatabaseInitializer(args)
        success = await initializer.initialize_all()

        # 更新状态显示
        await self._update_db_status()

    async def _check_db_connections(self):
        """仅检查数据库连接"""
        from scripts.init_database import DatabaseInitializer
        import argparse

        args = argparse.Namespace(
            check_only=True,
            with_sample_data=False,
            skip_postgres=False,
            skip_nebula=False,
            skip_clickhouse=False,
            skip_qdrant=False,
            force=False,
            wait_docker=0
        )

        initializer = DatabaseInitializer(args)
        await initializer.check_all_connections()
        await self._update_db_status()

    async def _check_db_status(self):
        """检查数据库状态"""
        await self._check_db_connections()

    async def _update_db_status(self):
        """更新数据库状态显示"""
        status_text = ""

        try:
            from scripts.check_system import (
                check_postgresql, check_nebulagraph,
                check_qdrant, check_clickhouse
            )

            ok, msg = await check_postgresql()
            status_text += f"PostgreSQL:  {'✅' if ok else '❌'} {msg}\n"

            ok, msg = await check_nebulagraph()
            status_text += f"NebulaGraph: {'✅' if ok else '❌'} {msg}\n"

            ok, msg = await check_clickhouse()
            status_text += f"ClickHouse:  {'✅' if ok else '❌'} {msg}\n"

            ok, msg = await check_qdrant()
            status_text += f"Qdrant:      {'✅' if ok else '❌'} {msg}\n"

        except Exception as e:
            status_text = f"获取状态失败: {e}"

        # 更新UI
        self.root.after(0, lambda: self._update_db_status_ui(status_text))

    def _update_db_status_ui(self, text):
        """更新数据库状态UI"""
        self.db_status_text.config(state=tk.NORMAL)
        self.db_status_text.delete(1.0, tk.END)
        self.db_status_text.insert(tk.END, text)
        self.db_status_text.config(state=tk.DISABLED)

    async def _import_initial_data(self):
        """导入初始数据"""
        self._log("=" * 50)
        self._log("开始导入初始数据...")
        self._log("=" * 50)

        from scripts.import_data import DataImporter

        importer = DataImporter()
        try:
            await importer.init()
            await importer.import_initial()
        finally:
            await importer.close()

    async def _import_data(self):
        """导入数据"""
        import_type = self.import_type.get()

        self._log("=" * 50)
        self._log(f"开始{import_type}导入...")
        self._log("=" * 50)

        from scripts.import_data import DataImporter

        importer = DataImporter()
        try:
            await importer.init()
            if import_type == "initial":
                await importer.import_initial()
            else:
                await importer.process_incremental()
        finally:
            await importer.close()

    async def _run_reminders(self):
        """运行生日回访"""
        self._log("=" * 50)
        self._log("开始运行生日回访任务...")
        self._log("=" * 50)

        from scripts.run_reminders import ReminderRunner

        runner = ReminderRunner(test_mode=True)
        try:
            await runner.init()
            await runner.run_reminders()
        finally:
            await runner.close()

    async def _run_reminder_task(self):
        """运行回访任务（带选项）"""
        institution = self.selected_institution.get()
        test_mode = self.test_mode.get()
        report_only = self.report_only.get()

        self._log("=" * 50)
        self._log(f"开始运行生日回访任务...")
        self._log(f"  机构: {institution}")
        self._log(f"  测试模式: {test_mode}")
        self._log(f"  仅报告: {report_only}")
        self._log("=" * 50)

        from scripts.run_reminders import ReminderRunner

        runner = ReminderRunner(test_mode=test_mode)
        try:
            await runner.init()

            if report_only:
                await runner.show_report(
                    None if institution == "all" else institution
                )
            else:
                await runner.run_reminders(
                    None if institution == "all" else institution
                )
        finally:
            await runner.close()

    async def _show_today_birthdays(self):
        """显示今日生日客户"""
        self._log("=" * 50)
        self._log("查询今日生日客户...")
        self._log("=" * 50)

        from services.data_sync import DataSyncService
        from config.settings import settings

        service = DataSyncService()
        try:
            await service.init_connections()

            for inst_code in settings.APP.INSTITUTIONS:
                self._log(f"\n机构 {inst_code}:")
                customers = await service.get_upcoming_birthday_customers(inst_code, 0)

                if customers:
                    for c in customers:
                        self._log(f"  - {c.get('name')} ({c.get('customer_code')})")
                else:
                    self._log("  无今日生日客户")

        finally:
            await service.close_connections()

    def _start_api_service(self):
        """启动API服务"""
        import subprocess

        try:
            from config.settings import settings

            self._log("正在启动API服务...")
            self._log(f"地址: http://{settings.API.HOST}:{settings.API.PORT}")

            # 在新进程中启动
            cmd = [
                sys.executable, "-m", "uvicorn",
                "api.main:app",
                "--host", settings.API.HOST,
                "--port", str(settings.API.PORT),
                "--reload"
            ]

            subprocess.Popen(
                cmd,
                cwd=str(PROJECT_ROOT),
                creationflags=subprocess.CREATE_NEW_CONSOLE
            )

            self._log("✅ API服务已在新窗口启动")
            self._log("文档地址: http://localhost:8000/docs")

        except Exception as e:
            self._log(f"❌ 启动API服务失败: {e}")

    def _open_api_docs(self):
        """打开API文档"""
        import webbrowser
        try:
            from config.settings import settings
            url = f"http://{settings.API.HOST}:{settings.API.PORT}/docs"
            webbrowser.open(url)
        except Exception as e:
            messagebox.showerror("错误", f"无法打开浏览器: {e}")

    def _open_data_dir(self):
        """打开数据目录"""
        import os
        data_dir = PROJECT_ROOT / "data" / "import"
        os.startfile(str(data_dir))

    def run(self):
        """运行应用"""
        self.root.mainloop()


def main():
    """主函数"""
    app = RevisitLauncher()
    app.run()


if __name__ == "__main__":
    main()

